/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItem;
import org.whispersystems.textsecuregcm.entities.PreKeySignatureValidator;
import org.whispersystems.textsecuregcm.entities.SetKeysRequest;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
@Tag(name = "Keys")
public class KeysController {

  private final RateLimiters rateLimiters;
  private final KeysManager keysManager;
  private final AccountsManager accounts;

  private static final String GET_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "getKeys");

  private static final CompletableFuture<?>[] EMPTY_FUTURE_ARRAY = new CompletableFuture[0];

  public KeysController(RateLimiters rateLimiters, KeysManager keysManager, AccountsManager accounts) {
    this.rateLimiters = rateLimiters;
    this.keysManager = keysManager;
    this.accounts = accounts;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get prekey count",
      description = "Gets the number of one-time prekeys uploaded for this device and still available")
  @ApiResponse(responseCode = "200", description = "Body contains the number of available one-time prekeys for the device.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public CompletableFuture<PreKeyCount> getStatus(@Auth final AuthenticatedAccount auth,
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType) {

    final CompletableFuture<Integer> ecCountFuture =
        keysManager.getEcCount(auth.getAccount().getIdentifier(identityType), auth.getAuthenticatedDevice().getId());

    final CompletableFuture<Integer> pqCountFuture =
        keysManager.getPqCount(auth.getAccount().getIdentifier(identityType), auth.getAuthenticatedDevice().getId());

    return ecCountFuture.thenCombine(pqCountFuture, PreKeyCount::new);
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Upload new prekeys", description = "Upload new pre-keys for this device.")
  @ApiResponse(responseCode = "200", description = "Indicates that new keys were successfully stored.")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "403", description = "Attempt to change identity key from a non-primary device.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  public CompletableFuture<Response> setKeys(@Auth final AuthenticatedAccount auth,
      @RequestBody @NotNull @Valid final SetKeysRequest setKeysRequest,

      @Parameter(allowEmptyValue=true)
      @Schema(
          allowableValues={"aci", "pni"},
          defaultValue="aci",
          description="whether this operation applies to the account (aci) or phone-number (pni) identity")
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType) {

    final Account account = auth.getAccount();
    final Device device = auth.getAuthenticatedDevice();
    final UUID identifier = account.getIdentifier(identityType);

    checkSignedPreKeySignatures(setKeysRequest, account.getIdentityKey(identityType));

    final List<CompletableFuture<Void>> storeFutures = new ArrayList<>(4);

    if (setKeysRequest.preKeys() != null && !setKeysRequest.preKeys().isEmpty()) {
      storeFutures.add(keysManager.storeEcOneTimePreKeys(identifier, device.getId(), setKeysRequest.preKeys()));
    }

    if (setKeysRequest.signedPreKey() != null) {
      storeFutures.add(keysManager.storeEcSignedPreKeys(identifier, device.getId(), setKeysRequest.signedPreKey()));
    }

    if (setKeysRequest.pqPreKeys() != null && !setKeysRequest.pqPreKeys().isEmpty()) {
      storeFutures.add(keysManager.storeKemOneTimePreKeys(identifier, device.getId(), setKeysRequest.pqPreKeys()));
    }

    if (setKeysRequest.pqLastResortPreKey() != null) {
      storeFutures.add(keysManager.storePqLastResort(identifier, device.getId(), setKeysRequest.pqLastResortPreKey()));
    }

    return CompletableFuture.allOf(storeFutures.toArray(EMPTY_FUTURE_ARRAY))
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  private void checkSignedPreKeySignatures(final SetKeysRequest setKeysRequest, final IdentityKey identityKey) {
    final List<SignedPreKey<?>> signedPreKeys = new ArrayList<>();

    if (setKeysRequest.pqPreKeys() != null) {
      signedPreKeys.addAll(setKeysRequest.pqPreKeys());
    }

    if (setKeysRequest.pqLastResortPreKey() != null) {
      signedPreKeys.add(setKeysRequest.pqLastResortPreKey());
    }

    if (setKeysRequest.signedPreKey() != null) {
      signedPreKeys.add(setKeysRequest.signedPreKey());
    }

    final boolean allSignaturesValid =
        signedPreKeys.isEmpty() || PreKeySignatureValidator.validatePreKeySignatures(identityKey, signedPreKeys);

    if (!allSignaturesValid) {
      throw new WebApplicationException("Invalid signature", 422);
    }
  }

  @GET
  @Path("/{identifier}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Fetch public keys for another user",
      description = "Retrieves the public identity key and available device prekeys for a specified account or phone-number identity")
  @ApiResponse(responseCode = "200", description = "Indicates at least one prekey was available for at least one requested device.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed and unidentified-access key was not supplied or invalid.")
  @ApiResponse(responseCode = "404", description = "Requested identity or device does not exist, is not active, or has no available prekeys.")
  @ApiResponse(responseCode = "429", description = "Rate limit exceeded.", headers = @Header(
      name = "Retry-After",
      description = "If present, a positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public PreKeyResponse getDeviceKeys(@Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) Optional<Anonymous> accessKey,

      @Parameter(description="the account or phone-number identifier to retrieve keys for")
      @PathParam("identifier") ServiceIdentifier targetIdentifier,

      @Parameter(description="the device id of a single device to retrieve prekeys for, or `*` for all enabled devices")
      @PathParam("device_id") String deviceId,

      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent)
      throws RateLimitExceededException {

    if (auth.isEmpty() && accessKey.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    final Optional<Account> account = auth.map(AuthenticatedAccount::getAccount);

    final Account target;
    {
      final Optional<Account> maybeTarget = accounts.getByServiceIdentifier(targetIdentifier);

      OptionalAccess.verify(account, accessKey, maybeTarget, deviceId);

      target = maybeTarget.orElseThrow();
    }

    if (account.isPresent()) {
      rateLimiters.getPreKeysLimiter().validate(
          account.get().getUuid() + "." + auth.get().getAuthenticatedDevice().getId() + "__" + targetIdentifier.uuid()
              + "." + deviceId);
    }

    final List<Device> devices = parseDeviceId(deviceId, target);
    final List<PreKeyResponseItem> responseItems = new ArrayList<>(devices.size());

    final List<CompletableFuture<Void>> tasks = devices.stream().map(device -> {
          final CompletableFuture<Optional<ECPreKey>> unsignedEcPreKeyFuture =
              keysManager.takeEC(targetIdentifier.uuid(), device.getId());

          final CompletableFuture<Optional<ECSignedPreKey>> signedEcPreKeyFuture =
              keysManager.getEcSignedPreKey(targetIdentifier.uuid(), device.getId());

          final CompletableFuture<Optional<KEMSignedPreKey>> pqPreKeyFuture =
              keysManager.takePQ(targetIdentifier.uuid(), device.getId());

          return CompletableFuture.allOf(unsignedEcPreKeyFuture, signedEcPreKeyFuture, pqPreKeyFuture)
              .thenAccept(ignored -> {
                final KEMSignedPreKey pqPreKey = pqPreKeyFuture.join().orElse(null);
                final ECPreKey unsignedEcPreKey = unsignedEcPreKeyFuture.join().orElse(null);
                final ECSignedPreKey signedEcPreKey = signedEcPreKeyFuture.join().orElse(null);

                Metrics.counter(GET_KEYS_COUNTER_NAME, Tags.of(
                        io.micrometer.core.instrument.Tag.of("isPrimary", String.valueOf(device.isPrimary())),
                        UserAgentTagUtil.getPlatformTag(userAgent),
                        io.micrometer.core.instrument.Tag.of("targetPlatform", getDevicePlatform(device).map(Enum::name).orElse("unknown")),
                        io.micrometer.core.instrument.Tag.of("identityType", targetIdentifier.identityType().name()),
                        io.micrometer.core.instrument.Tag.of("isStale", String.valueOf(isDeviceStale(device))),
                        io.micrometer.core.instrument.Tag.of("oneTimeEcKeyAvailable", String.valueOf(unsignedEcPreKey == null))))
                    .increment();

                if (signedEcPreKey != null || unsignedEcPreKey != null || pqPreKey != null) {
                  final int registrationId = switch (targetIdentifier.identityType()) {
                    case ACI -> device.getRegistrationId();
                    case PNI -> device.getPhoneNumberIdentityRegistrationId().orElse(device.getRegistrationId());
                  };

                  responseItems.add(
                      new PreKeyResponseItem(device.getId(), registrationId, signedEcPreKey, unsignedEcPreKey,
                          pqPreKey));
                }
              });
        })
        .toList();

    CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

    final IdentityKey identityKey = target.getIdentityKey(targetIdentifier.identityType());

    if (responseItems.isEmpty()) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }

    return new PreKeyResponse(identityKey, responseItems);
  }

  private static Optional<ClientPlatform> getDevicePlatform(final Device device) {
    if (StringUtils.isNotBlank(device.getApnId()) || StringUtils.isNotBlank(device.getVoipApnId())) {
      return Optional.of(ClientPlatform.IOS);
    } else if (StringUtils.isNotBlank(device.getGcmId())) {
      return Optional.of(ClientPlatform.ANDROID);
    }

    return Optional.empty();
  }

  private static boolean isDeviceStale(final Device device) {
    return Duration.between(Instant.ofEpochMilli(device.getLastSeen()), Instant.now())
        .compareTo(Duration.ofDays(30)) >= 0;
  }

  @PUT
  @Path("/signed")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "Upload a new signed prekey",
      description = """
          Upload a new signed elliptic-curve prekey for this device. Deprecated; use PUT /v2/keys instead.
      """)
  @ApiResponse(responseCode = "200", description = "Indicates that new prekey was successfully stored.")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  public CompletableFuture<Response> setSignedKey(@Auth final AuthenticatedAccount auth,
      @Valid final ECSignedPreKey signedPreKey,
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType) {

    final UUID identifier = auth.getAccount().getIdentifier(identityType);
    final byte deviceId = auth.getAuthenticatedDevice().getId();

    return keysManager.storeEcSignedPreKeys(identifier, deviceId, signedPreKey)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  private List<Device> parseDeviceId(String deviceId, Account account) {
    if (deviceId.equals("*")) {
      return account.getDevices().stream().filter(Device::isEnabled).toList();
    }
    try {
      byte id = Byte.parseByte(deviceId);
      return account.getDevice(id).filter(Device::isEnabled).map(List::of).orElse(List.of());
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }
}
