/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.lettuce.core.SetArgs;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
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
import org.whispersystems.textsecuregcm.auth.BasicAuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.ChangesDeviceEnabledState;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceActivationRequest;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.entities.DeviceInfoList;
import org.whispersystems.textsecuregcm.entities.DeviceResponse;
import org.whispersystems.textsecuregcm.entities.LinkDeviceRequest;
import org.whispersystems.textsecuregcm.entities.PreKeySignatureValidator;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Device.DeviceCapabilities;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.VerificationCode;

@Path("/v1/devices")
@Tag(name = "Devices")
public class DeviceController {

  static final int MAX_DEVICES = 6;

  private final Key verificationTokenKey;
  private final AccountsManager       accounts;
  private final MessagesManager       messages;
  private final KeysManager keys;
  private final RateLimiters          rateLimiters;
  private final FaultTolerantRedisCluster usedTokenCluster;
  private final Map<String, Integer>  maxDeviceConfiguration;

  private final Clock clock;

  private static final String VERIFICATION_TOKEN_ALGORITHM = "HmacSHA256";

  @VisibleForTesting
  static final Duration TOKEN_EXPIRATION_DURATION = Duration.ofMinutes(10);

  public DeviceController(byte[] linkDeviceSecret,
      AccountsManager accounts,
      MessagesManager messages,
      KeysManager keys,
      RateLimiters rateLimiters,
      FaultTolerantRedisCluster usedTokenCluster,
      Map<String, Integer> maxDeviceConfiguration, final Clock clock) {
    this.verificationTokenKey = new SecretKeySpec(linkDeviceSecret, VERIFICATION_TOKEN_ALGORITHM);
    this.accounts = accounts;
    this.messages = messages;
    this.keys = keys;
    this.rateLimiters = rateLimiters;
    this.usedTokenCluster = usedTokenCluster;
    this.maxDeviceConfiguration = maxDeviceConfiguration;
    this.clock = clock;

    // Fail fast: reject bad keys
    try {
      final Mac mac = Mac.getInstance(VERIFICATION_TOKEN_ALGORITHM);
      mac.init(verificationTokenKey);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("All Java implementations must support HmacSHA256", e);
    } catch (final InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }
  }

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

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{device_id}")
  @ChangesDeviceEnabledState
  public void removeDevice(@Auth AuthenticatedAccount auth, @PathParam("device_id") long deviceId) {
    Account account = auth.getAccount();
    if (auth.getAuthenticatedDevice().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (deviceId == Device.MASTER_ID) {
      throw new ForbiddenException();
    }

    final CompletableFuture<Void> deleteKeysFuture = keys.delete(account.getUuid(), deviceId);

    messages.clear(account.getUuid(), deviceId).join();
    account = accounts.update(account, a -> a.removeDevice(deviceId));
    // ensure any messages that came in after the first clear() are also removed
    messages.clear(account.getUuid(), deviceId).join();

    deleteKeysFuture.join();
  }

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

    return new VerificationCode(generateVerificationToken(account.getUuid()));
  }

  /**
   * @deprecated callers should use {@link #linkDevice(BasicAuthorizationHeader, LinkDeviceRequest, ContainerRequest)}
   * instead
   */
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{verification_code}")
  @ChangesDeviceEnabledState
  @Deprecated(forRemoval = true)
  public DeviceResponse verifyDeviceToken(@PathParam("verification_code") String verificationCode,
      @HeaderParam(HttpHeaders.AUTHORIZATION) BasicAuthorizationHeader authorizationHeader,
      @NotNull @Valid AccountAttributes accountAttributes,
      @Context ContainerRequest containerRequest)
      throws RateLimitExceededException, DeviceLimitExceededException {

    final Pair<Account, Device> accountAndDevice = createDevice(authorizationHeader.getPassword(),
        verificationCode,
        accountAttributes,
        containerRequest,
        Optional.empty());

    final Account account = accountAndDevice.first();
    final Device device = accountAndDevice.second();

    return new DeviceResponse(account.getUuid(), account.getPhoneNumberIdentifier(), device.getId());
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/link")
  @ChangesDeviceEnabledState
  @Operation(summary = "Link a device to an account",
      description = """
      Links a device to an account identified by a given phone number.
      """)
  @ApiResponse(responseCode = "200", description = "The new device was linked to the calling account", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "403", description = "The given account was not found or the given verification code was incorrect")
  @ApiResponse(responseCode = "411", description = "The given account already has its maximum number of linked devices")
  @ApiResponse(responseCode = "422", description = "The request did not pass validation")
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public DeviceResponse linkDevice(@HeaderParam(HttpHeaders.AUTHORIZATION) BasicAuthorizationHeader authorizationHeader,
                                   @NotNull @Valid LinkDeviceRequest linkDeviceRequest,
                                   @Context ContainerRequest containerRequest)
      throws RateLimitExceededException, DeviceLimitExceededException {

    final Pair<Account, Device> accountAndDevice = createDevice(authorizationHeader.getPassword(),
        linkDeviceRequest.verificationCode(),
        linkDeviceRequest.accountAttributes(),
        containerRequest,
        Optional.of(linkDeviceRequest.deviceActivationRequest()));

    final Account account = accountAndDevice.first();
    final Device device = accountAndDevice.second();

    return new DeviceResponse(account.getUuid(), account.getPhoneNumberIdentifier(), device.getId());
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/unauthenticated_delivery")
  public void setUnauthenticatedDelivery(@Auth AuthenticatedAccount auth) {
    assert (auth.getAuthenticatedDevice() != null);
    // Deprecated
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/capabilities")
  public void setCapabilities(@Auth AuthenticatedAccount auth, @NotNull @Valid DeviceCapabilities capabilities) {
    assert (auth.getAuthenticatedDevice() != null);
    final long deviceId = auth.getAuthenticatedDevice().getId();
    accounts.updateDevice(auth.getAccount(), deviceId, d -> d.setCapabilities(capabilities));
  }

  private Mac getInitializedMac() {
    try {
      final Mac mac = Mac.getInstance(VERIFICATION_TOKEN_ALGORITHM);
      mac.init(verificationTokenKey);

      return mac;
    } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
      // All Java implementations must support HmacSHA256 and we checked the key at construction time, so this can never
      // happen
      throw new AssertionError(e);
    }
  }

  @VisibleForTesting
  String generateVerificationToken(final UUID aci) {
    final String claims = aci + "." + clock.instant().toEpochMilli();
    final byte[] signature = getInitializedMac().doFinal(claims.getBytes(StandardCharsets.UTF_8));

    return claims + ":" + Base64.getUrlEncoder().encodeToString(signature);
  }

  @VisibleForTesting
  Optional<UUID> checkVerificationToken(final String verificationToken) {
    final boolean tokenUsed = usedTokenCluster.withCluster(connection ->
        connection.sync().get(getUsedTokenKey(verificationToken)) != null);

    if (tokenUsed) {
      return Optional.empty();
    }

    final String[] claimsAndSignature = verificationToken.split(":", 2);

    if (claimsAndSignature.length != 2) {
      return Optional.empty();
    }

    final byte[] expectedSignature = getInitializedMac().doFinal(claimsAndSignature[0].getBytes(StandardCharsets.UTF_8));
    final byte[] providedSignature;

    try {
      providedSignature = Base64.getUrlDecoder().decode(claimsAndSignature[1]);
    } catch (final IllegalArgumentException e) {
      return Optional.empty();
    }

    if (!MessageDigest.isEqual(expectedSignature, providedSignature)) {
      return Optional.empty();
    }

    final String[] aciAndTimestamp = claimsAndSignature[0].split("\\.", 2);

    if (aciAndTimestamp.length != 2) {
      return Optional.empty();
    }

    final UUID aci;

    try {
      aci = UUID.fromString(aciAndTimestamp[0]);
    } catch (final IllegalArgumentException e) {
      return Optional.empty();
    }

    final Instant timestamp;

    try {
      timestamp = Instant.ofEpochMilli(Long.parseLong(aciAndTimestamp[1]));
    } catch (final NumberFormatException e) {
      return Optional.empty();
    }

    final Instant tokenExpiration = timestamp.plus(TOKEN_EXPIRATION_DURATION);

    if (tokenExpiration.isBefore(clock.instant())) {
      return Optional.empty();
    }

    return Optional.of(aci);
  }

  static boolean isCapabilityDowngrade(Account account, DeviceCapabilities capabilities) {
    return account.isPniSupported() && !capabilities.pni();
  }

  private Pair<Account, Device> createDevice(final String password,
                                     final String verificationCode,
                                     final AccountAttributes accountAttributes,
                                     final ContainerRequest containerRequest,
                                     final Optional<DeviceActivationRequest> maybeDeviceActivationRequest)
      throws RateLimitExceededException, DeviceLimitExceededException {

    final Optional<UUID> maybeAciFromToken = checkVerificationToken(verificationCode);

    final Account account = maybeAciFromToken.flatMap(accounts::getByAccountIdentifier)
        .orElseThrow(ForbiddenException::new);

    rateLimiters.getVerifyDeviceLimiter().validate(account.getUuid());

    maybeDeviceActivationRequest.ifPresent(deviceActivationRequest -> {
      assert deviceActivationRequest.aciSignedPreKey().isPresent();
      assert deviceActivationRequest.pniSignedPreKey().isPresent();
      assert deviceActivationRequest.aciPqLastResortPreKey().isPresent();
      assert deviceActivationRequest.pniPqLastResortPreKey().isPresent();

      final boolean allKeysValid = PreKeySignatureValidator.validatePreKeySignatures(account.getIdentityKey(
              IdentityType.ACI),
          List.of(deviceActivationRequest.aciSignedPreKey().get(), deviceActivationRequest.aciPqLastResortPreKey().get()))
          && PreKeySignatureValidator.validatePreKeySignatures(account.getIdentityKey(IdentityType.PNI),
          List.of(deviceActivationRequest.pniSignedPreKey().get(), deviceActivationRequest.pniPqLastResortPreKey().get()));

      if (!allKeysValid) {
        throw new WebApplicationException(Response.status(422).build());
      }
    });

    // Normally, the "do we need to refresh somebody's websockets" listener can do this on its own. In this case,
    // we're not using the conventional authentication system, and so we need to give it a hint so it knows who the
    // active user is and what their device states look like.
    AuthEnablementRefreshRequirementProvider.setAccount(containerRequest, account);

    int maxDeviceLimit = MAX_DEVICES;

    if (maxDeviceConfiguration.containsKey(account.getNumber())) {
      maxDeviceLimit = maxDeviceConfiguration.get(account.getNumber());
    }

    if (account.getEnabledDeviceCount() >= maxDeviceLimit) {
      throw new DeviceLimitExceededException(account.getDevices().size(), MAX_DEVICES);
    }

    final DeviceCapabilities capabilities = accountAttributes.getCapabilities();
    if (capabilities != null && isCapabilityDowngrade(account, capabilities)) {
      throw new WebApplicationException(Response.status(409).build());
    }

    final Device device = new Device();
    device.setName(accountAttributes.getName());
    device.setAuthTokenHash(SaltedTokenHash.generateFor(password));
    device.setFetchesMessages(accountAttributes.getFetchesMessages());
    device.setRegistrationId(accountAttributes.getRegistrationId());
    accountAttributes.getPhoneNumberIdentityRegistrationId().ifPresent(device::setPhoneNumberIdentityRegistrationId);
    device.setLastSeen(Util.todayInMillis());
    device.setCreated(System.currentTimeMillis());
    device.setCapabilities(accountAttributes.getCapabilities());

    maybeDeviceActivationRequest.ifPresent(deviceActivationRequest -> {
      device.setSignedPreKey(deviceActivationRequest.aciSignedPreKey().get());
      device.setPhoneNumberIdentitySignedPreKey(deviceActivationRequest.pniSignedPreKey().get());

      deviceActivationRequest.apnToken().ifPresent(apnRegistrationId -> {
        device.setApnId(apnRegistrationId.apnRegistrationId());
        device.setVoipApnId(apnRegistrationId.voipRegistrationId());
      });

      deviceActivationRequest.gcmToken().ifPresent(gcmRegistrationId ->
          device.setGcmId(gcmRegistrationId.gcmRegistrationId()));
    });

    final Account updatedAccount = accounts.update(account, a -> {
      device.setId(a.getNextDeviceId());

      final CompletableFuture<Void> deleteKeysFuture = CompletableFuture.allOf(
          keys.delete(a.getUuid(), device.getId()),
          keys.delete(a.getPhoneNumberIdentifier(), device.getId()));

      messages.clear(a.getUuid(), device.getId()).join();

      deleteKeysFuture.join();

      maybeDeviceActivationRequest.ifPresent(deviceActivationRequest -> CompletableFuture.allOf(
              keys.storeEcSignedPreKeys(a.getUuid(),
                  Map.of(device.getId(), deviceActivationRequest.aciSignedPreKey().get())),
              keys.storePqLastResort(a.getUuid(),
                  Map.of(device.getId(), deviceActivationRequest.aciPqLastResortPreKey().get())),
              keys.storeEcSignedPreKeys(a.getPhoneNumberIdentifier(),
                  Map.of(device.getId(), deviceActivationRequest.pniSignedPreKey().get())),
              keys.storePqLastResort(a.getPhoneNumberIdentifier(),
                  Map.of(device.getId(), deviceActivationRequest.pniPqLastResortPreKey().get())))
          .join());

      a.addDevice(device);
    });

    if (maybeAciFromToken.isPresent()) {
      usedTokenCluster.useCluster(connection ->
          connection.sync().set(getUsedTokenKey(verificationCode), "", new SetArgs().ex(TOKEN_EXPIRATION_DURATION)));
    }

    return new Pair<>(updatedAccount, device);
  }

  private static String getUsedTokenKey(final String token) {
    return "usedToken::" + token;
  }
}
