/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.entities.DeviceInfoList;
import org.whispersystems.textsecuregcm.entities.DeviceResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.VerificationCode;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Path("/v1/devices")
public class DeviceController {

  private final Logger logger = LoggerFactory.getLogger(DeviceController.class);

  private static final int MAX_DEVICES = 6;

  private final PendingDevicesManager pendingDevices;
  private final AccountsManager       accounts;
  private final MessagesManager       messages;
  private final RateLimiters          rateLimiters;
  private final Map<String, Integer>  maxDeviceConfiguration;
  private final DirectoryQueue        directoryQueue;

  public DeviceController(PendingDevicesManager pendingDevices,
                          AccountsManager accounts,
                          MessagesManager messages,
                          DirectoryQueue directoryQueue,
                          RateLimiters rateLimiters,
                          Map<String, Integer> maxDeviceConfiguration)
  {
    this.pendingDevices         = pendingDevices;
    this.accounts               = accounts;
    this.messages               = messages;
    this.directoryQueue         = directoryQueue;
    this.rateLimiters           = rateLimiters;
    this.maxDeviceConfiguration = maxDeviceConfiguration;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public DeviceInfoList getDevices(@Auth Account account) {
    List<DeviceInfo> devices = new LinkedList<>();

    for (Device device : account.getDevices()) {
      devices.add(new DeviceInfo(device.getId(), device.getName(),
                                 device.getLastSeen(), device.getCreated()));
    }

    return new DeviceInfoList(devices);
  }

  @Timed
  @DELETE
  @Path("/{device_id}")
  public void removeDevice(@Auth Account account, @PathParam("device_id") long deviceId) {
    if (account.getAuthenticatedDevice().get().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    account.removeDevice(deviceId);
    accounts.update(account);
    directoryQueue.refreshRegisteredUser(account);
    messages.clear(account.getNumber(), account.getUuid(), deviceId);
  }

  @Timed
  @GET
  @Path("/provisioning/code")
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationCode createDeviceToken(@Auth Account account)
      throws RateLimitExceededException, DeviceLimitExceededException
  {
    rateLimiters.getAllocateDeviceLimiter().validate(account.getNumber());

    int maxDeviceLimit = MAX_DEVICES;

    if (maxDeviceConfiguration.containsKey(account.getNumber())) {
      maxDeviceLimit = maxDeviceConfiguration.get(account.getNumber());
    }

    if (account.getEnabledDeviceCount() >= maxDeviceLimit) {
      throw new DeviceLimitExceededException(account.getDevices().size(), MAX_DEVICES);
    }

    if (account.getAuthenticatedDevice().get().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    VerificationCode       verificationCode       = generateVerificationCode();
    StoredVerificationCode storedVerificationCode = new StoredVerificationCode(verificationCode.getVerificationCode(),
                                                                               System.currentTimeMillis(),
                                                                               null);

    pendingDevices.store(account.getNumber(), storedVerificationCode);

    return verificationCode;
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{verification_code}")
  public DeviceResponse verifyDeviceToken(@PathParam("verification_code") String verificationCode,
                                          @HeaderParam("Authorization")   String authorizationHeader,
                                          @HeaderParam("User-Agent")      String userAgent,
                                          @Valid                          AccountAttributes accountAttributes)
      throws RateLimitExceededException, DeviceLimitExceededException
  {
    try {
      AuthorizationHeader header = AuthorizationHeader.fromFullHeader(authorizationHeader);
      String number              = header.getIdentifier().getNumber();
      String password            = header.getPassword();

      if (number == null) throw new WebApplicationException(400);

      rateLimiters.getVerifyDeviceLimiter().validate(number);

      Optional<StoredVerificationCode> storedVerificationCode = pendingDevices.getCodeForNumber(number);

      if (!storedVerificationCode.isPresent() || !storedVerificationCode.get().isValid(verificationCode)) {
        throw new WebApplicationException(Response.status(403).build());
      }

      Optional<Account> account = accounts.get(number);

      if (!account.isPresent()) {
        throw new WebApplicationException(Response.status(403).build());
      }

      int maxDeviceLimit = MAX_DEVICES;

      if (maxDeviceConfiguration.containsKey(account.get().getNumber())) {
        maxDeviceLimit = maxDeviceConfiguration.get(account.get().getNumber());
      }

      if (account.get().getEnabledDeviceCount() >= maxDeviceLimit) {
        throw new DeviceLimitExceededException(account.get().getDevices().size(), MAX_DEVICES);
      }

      final DeviceCapabilities capabilities = accountAttributes.getCapabilities();
      if (capabilities != null && isCapabilityDowngrade(account.get(), capabilities, userAgent)) {
        throw new WebApplicationException(Response.status(409).build());
      }

      Device device = new Device();
      device.setName(accountAttributes.getName());
      device.setAuthenticationCredentials(new AuthenticationCredentials(password));
      device.setSignalingKey(accountAttributes.getSignalingKey());
      device.setFetchesMessages(accountAttributes.getFetchesMessages());
      device.setId(account.get().getNextDeviceId());
      device.setRegistrationId(accountAttributes.getRegistrationId());
      device.setLastSeen(Util.todayInMillis());
      device.setCreated(System.currentTimeMillis());
      device.setCapabilities(accountAttributes.getCapabilities());

      account.get().addDevice(device);
      messages.clear(account.get().getNumber(), account.get().getUuid(), device.getId());
      accounts.update(account.get());

      pendingDevices.remove(number);

      return new DeviceResponse(device.getId());
    } catch (InvalidAuthorizationHeaderException e) {
      logger.info("Bad Authorization Header", e);
      throw new WebApplicationException(Response.status(401).build());
    }
  }

  @Timed
  @PUT
  @Path("/unauthenticated_delivery")
  public void setUnauthenticatedDelivery(@Auth Account account) {
    assert(account.getAuthenticatedDevice().isPresent());
    // Deprecated
  }

  @Timed
  @PUT
  @Path("/capabilities")
  public void setCapabiltities(@Auth Account account, @Valid DeviceCapabilities capabilities) {
    assert(account.getAuthenticatedDevice().isPresent());
    account.getAuthenticatedDevice().get().setCapabilities(capabilities);
    accounts.update(account);
  }

  @VisibleForTesting protected VerificationCode generateVerificationCode() {
    SecureRandom random = new SecureRandom();
    int randomInt       = 100000 + random.nextInt(900000);
    return new VerificationCode(randomInt);
  }

  private boolean isCapabilityDowngrade(Account account, DeviceCapabilities capabilities, String userAgent) {
    boolean isDowngrade = false;

    if (account.isGv1MigrationSupported() && !capabilities.isGv1Migration()) {
      isDowngrade = true;
    }

    if (account.isGroupsV2Supported()) {
      try {
        switch (UserAgentUtil.parseUserAgentString(userAgent).getPlatform()) {
          case DESKTOP:
          case ANDROID: {
            if (!capabilities.isGv2_3()) {
              isDowngrade = true;
            }

            break;
          }

          case IOS: {
            if (!capabilities.isGv2_2() && !capabilities.isGv2_3()) {
              isDowngrade = true;
            }

            break;
          }
        }
      } catch (final UnrecognizedUserAgentException e) {
        // If we can't parse the UA string, the client is for sure too old to support groups V2
        isDowngrade = true;
      }
    }

    return isDowngrade;
  }
}
