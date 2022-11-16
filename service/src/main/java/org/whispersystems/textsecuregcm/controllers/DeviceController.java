/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;
import org.whispersystems.textsecuregcm.auth.AuthEnablementRefreshRequirementProvider;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.BasicAuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.ChangesDeviceEnabledState;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.entities.DeviceInfoList;
import org.whispersystems.textsecuregcm.entities.DeviceResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.VerificationCode;

@Path("/v1/devices")
public class DeviceController {

  private static final int MAX_DEVICES = 6;

  private final StoredVerificationCodeManager pendingDevices;
  private final AccountsManager       accounts;
  private final MessagesManager       messages;
  private final Keys keys;
  private final RateLimiters          rateLimiters;
  private final Map<String, Integer>  maxDeviceConfiguration;

  public DeviceController(StoredVerificationCodeManager pendingDevices,
      AccountsManager accounts,
      MessagesManager messages,
      Keys keys,
      RateLimiters rateLimiters,
      Map<String, Integer> maxDeviceConfiguration) {
    this.pendingDevices = pendingDevices;
    this.accounts = accounts;
    this.messages = messages;
    this.keys = keys;
    this.rateLimiters = rateLimiters;
    this.maxDeviceConfiguration = maxDeviceConfiguration;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public DeviceInfoList getDevices(@Auth AuthenticatedAccount auth) {
    List<DeviceInfo> devices = new LinkedList<>();

    for (Device device : auth.getAccount().getDevices()) {
      devices.add(new DeviceInfo(device.getId(), device.getName(),
          device.getLastSeen(), device.getCreated()));
    }

    return new DeviceInfoList(devices);
  }

  @Timed
  @DELETE
  @Path("/{device_id}")
  @ChangesDeviceEnabledState
  public void removeDevice(@Auth AuthenticatedAccount auth, @PathParam("device_id") long deviceId) {
    Account account = auth.getAccount();
    if (auth.getAuthenticatedDevice().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    messages.clear(account.getUuid(), deviceId);
    account = accounts.update(account, a -> a.removeDevice(deviceId));
    keys.delete(account.getUuid(), deviceId);
    // ensure any messages that came in after the first clear() are also removed
    messages.clear(account.getUuid(), deviceId);
  }

  @Timed
  @GET
  @Path("/provisioning/code")
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationCode createDeviceToken(@Auth AuthenticatedAccount auth)
      throws RateLimitExceededException, DeviceLimitExceededException {

    final Account account = auth.getAccount();

    rateLimiters.getAllocateDeviceLimiter().validate(account.getUuid());

    int maxDeviceLimit = MAX_DEVICES;

    if (maxDeviceConfiguration.containsKey(account.getNumber())) {
      maxDeviceLimit = maxDeviceConfiguration.get(account.getNumber());
    }

    if (account.getEnabledDeviceCount() >= maxDeviceLimit) {
      throw new DeviceLimitExceededException(account.getDevices().size(), MAX_DEVICES);
    }

    if (auth.getAuthenticatedDevice().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    VerificationCode verificationCode = generateVerificationCode();
    StoredVerificationCode storedVerificationCode =
        new StoredVerificationCode(verificationCode.getVerificationCode(), System.currentTimeMillis(), null, null);

    pendingDevices.store(account.getNumber(), storedVerificationCode);

    return verificationCode;
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{verification_code}")
  @ChangesDeviceEnabledState
  public DeviceResponse verifyDeviceToken(@PathParam("verification_code") String verificationCode,
      @HeaderParam(HttpHeaders.AUTHORIZATION) BasicAuthorizationHeader authorizationHeader,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,
      @NotNull @Valid AccountAttributes accountAttributes,
      @Context ContainerRequest containerRequest)
      throws RateLimitExceededException, DeviceLimitExceededException {

    String number = authorizationHeader.getUsername();
    String password = authorizationHeader.getPassword();

    rateLimiters.getVerifyDeviceLimiter().validate(number);

    Optional<StoredVerificationCode> storedVerificationCode = pendingDevices.getCodeForNumber(number);

    if (storedVerificationCode.isEmpty() || !storedVerificationCode.get().isValid(verificationCode)) {
      throw new WebApplicationException(Response.status(403).build());
    }

    Optional<Account> account = accounts.getByE164(number);

    if (account.isEmpty()) {
      throw new WebApplicationException(Response.status(403).build());
    }

    // Normally, the "do we need to refresh somebody's websockets" listener can do this on its own. In this case,
    // we're not using the conventional authentication system, and so we need to give it a hint so it knows who the
    // active user is and what their device states look like.
    AuthEnablementRefreshRequirementProvider.setAccount(containerRequest, account.get());

    int maxDeviceLimit = MAX_DEVICES;

    if (maxDeviceConfiguration.containsKey(account.get().getNumber())) {
      maxDeviceLimit = maxDeviceConfiguration.get(account.get().getNumber());
    }

    if (account.get().getEnabledDeviceCount() >= maxDeviceLimit) {
      throw new DeviceLimitExceededException(account.get().getDevices().size(), MAX_DEVICES);
    }

    final DeviceCapabilities capabilities = accountAttributes.getCapabilities();
    if (capabilities != null && isCapabilityDowngrade(account.get(), capabilities)) {
      throw new WebApplicationException(Response.status(409).build());
    }

    Device device = new Device();
    device.setName(accountAttributes.getName());
    device.setAuthenticationCredentials(new AuthenticationCredentials(password));
    device.setFetchesMessages(accountAttributes.getFetchesMessages());
    device.setRegistrationId(accountAttributes.getRegistrationId());
    accountAttributes.getPhoneNumberIdentityRegistrationId().ifPresent(device::setPhoneNumberIdentityRegistrationId);
    device.setLastSeen(Util.todayInMillis());
    device.setCreated(System.currentTimeMillis());
    device.setCapabilities(accountAttributes.getCapabilities());

    final Account updatedAccount = accounts.update(account.get(), a -> {
      device.setId(a.getNextDeviceId());
      messages.clear(a.getUuid(), device.getId());
      a.addDevice(device);
    });

    pendingDevices.remove(number);

    return new DeviceResponse(updatedAccount.getUuid(), updatedAccount.getPhoneNumberIdentifier(), device.getId());
  }

  @Timed
  @PUT
  @Path("/unauthenticated_delivery")
  public void setUnauthenticatedDelivery(@Auth AuthenticatedAccount auth) {
    assert (auth.getAuthenticatedDevice() != null);
    // Deprecated
  }

  @Timed
  @PUT
  @Path("/capabilities")
  public void setCapabiltities(@Auth AuthenticatedAccount auth, @NotNull @Valid DeviceCapabilities capabilities) {
    assert (auth.getAuthenticatedDevice() != null);
    final long deviceId = auth.getAuthenticatedDevice().getId();
    accounts.updateDevice(auth.getAccount(), deviceId, d -> d.setCapabilities(capabilities));
  }

  @VisibleForTesting protected VerificationCode generateVerificationCode() {
    SecureRandom random = new SecureRandom();
    int randomInt       = 100000 + random.nextInt(900000);
    return new VerificationCode(randomInt);
  }

  private boolean isCapabilityDowngrade(Account account, DeviceCapabilities capabilities) {
    boolean isDowngrade = false;

    isDowngrade |= account.isStoriesSupported() && !capabilities.isStories();
    isDowngrade |= account.isPniSupported() && !capabilities.isPni();
    isDowngrade |= account.isChangeNumberSupported() && !capabilities.isChangeNumber();
    isDowngrade |= account.isAnnouncementGroupSupported() && !capabilities.isAnnouncementGroup();
    isDowngrade |= account.isSenderKeySupported() && !capabilities.isSenderKey();
    isDowngrade |= account.isGiftBadgesSupported() && !capabilities.isGiftBadges();

    return isDowngrade;
  }
}
