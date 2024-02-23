/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import javax.ws.rs.POST;
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
import org.whispersystems.textsecuregcm.entities.CheckKeysRequest;
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
import org.whispersystems.websocket.auth.ReadOnly;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Keys")
public class KeysController {

  private final RateLimiters rateLimiters;
  private final KeysManager keysManager;
  private final AccountsManager accounts;

  private static final String KEY_COUNT_DISTRIBUTION_NAME = MetricsUtil.name(KeysController.class, "getKeyCount");
  private static final String GET_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "getKeys");
  private static final String STORE_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "storeKeys");
  private static final String PRIMARY_DEVICE_TAG_NAME = "isPrimary";
  private static final String IDENTITY_TYPE_TAG_NAME = "identityType";
  private static final String KEY_TYPE_TAG_NAME = "keyType";

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
  public CompletableFuture<PreKeyCount> getStatus(@ReadOnly @Auth final AuthenticatedAccount auth,
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    final Tag platformTag = UserAgentTagUtil.getPlatformTag(userAgent);
    final Tag primaryDeviceTag = Tag.of(PRIMARY_DEVICE_TAG_NAME, String.valueOf(auth.getAuthenticatedDevice().isPrimary()));
    final Tag identityTypeTag = Tag.of(IDENTITY_TYPE_TAG_NAME, identityType.name());

    final CompletableFuture<Integer> ecCountFuture =
        keysManager.getEcCount(auth.getAccount().getIdentifier(identityType), auth.getAuthenticatedDevice().getId())
            .whenComplete((keyCount, throwable) -> {
              if (keyCount != null) {
                DistributionSummary.builder(KEY_COUNT_DISTRIBUTION_NAME)
                    .tags(Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "ec")))
                    .publishPercentileHistogram(true)
                    .register(Metrics.globalRegistry)
                    .record(keyCount);
              }
            });

    final CompletableFuture<Integer> pqCountFuture =
        keysManager.getPqCount(auth.getAccount().getIdentifier(identityType), auth.getAuthenticatedDevice().getId())
            .whenComplete((keyCount, throwable) -> {
              if (keyCount != null) {
                DistributionSummary.builder(KEY_COUNT_DISTRIBUTION_NAME)
                    .tags(Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "kyber")))
                    .publishPercentileHistogram(true)
                    .register(Metrics.globalRegistry)
                    .record(keyCount);
              }
            });

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
  public CompletableFuture<Response> setKeys(
      @ReadOnly @Auth final AuthenticatedAccount auth,
      @RequestBody @NotNull @Valid final SetKeysRequest setKeysRequest,

      @Parameter(allowEmptyValue=true)
      @Schema(
          allowableValues={"aci", "pni"},
          defaultValue="aci",
          description="whether this operation applies to the account (aci) or phone-number (pni) identity")
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    final Account account = auth.getAccount();
    final Device device = auth.getAuthenticatedDevice();
    final UUID identifier = account.getIdentifier(identityType);

    checkSignedPreKeySignatures(setKeysRequest, account.getIdentityKey(identityType));

    final Tag platformTag = UserAgentTagUtil.getPlatformTag(userAgent);
    final Tag primaryDeviceTag = Tag.of(PRIMARY_DEVICE_TAG_NAME, String.valueOf(auth.getAuthenticatedDevice().isPrimary()));
    final Tag identityTypeTag = Tag.of(IDENTITY_TYPE_TAG_NAME, identityType.name());

    final List<CompletableFuture<Void>> storeFutures = new ArrayList<>(4);

    if (setKeysRequest.preKeys() != null && !setKeysRequest.preKeys().isEmpty()) {
      Metrics.counter(STORE_KEYS_COUNTER_NAME,
          Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "ec")))
          .increment();

      storeFutures.add(keysManager.storeEcOneTimePreKeys(identifier, device.getId(), setKeysRequest.preKeys()));
    }

    if (setKeysRequest.signedPreKey() != null) {
      Metrics.counter(STORE_KEYS_COUNTER_NAME,
              Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "ec-signed")))
          .increment();

      storeFutures.add(keysManager.storeEcSignedPreKeys(identifier, device.getId(), setKeysRequest.signedPreKey()));
    }

    if (setKeysRequest.pqPreKeys() != null && !setKeysRequest.pqPreKeys().isEmpty()) {
      Metrics.counter(STORE_KEYS_COUNTER_NAME,
              Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "kyber")))
          .increment();

      storeFutures.add(keysManager.storeKemOneTimePreKeys(identifier, device.getId(), setKeysRequest.pqPreKeys()));
    }

    if (setKeysRequest.pqLastResortPreKey() != null) {
      Metrics.counter(STORE_KEYS_COUNTER_NAME,
              Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "kyber-last-resort")))
          .increment();

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

  @POST
  @Path("/check")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Check keys", description = """
      Checks that client and server have consistent views of repeated-use keys. For a given identity type, clients
      submit a digest of their repeated-use key material. The digest is calculated as:
      
      SHA256(identityKeyBytes || signedEcPreKeyId || signedEcPreKeyIdBytes || lastResortKeyId || lastResortKeyBytes)
      
      …where the elements of the hash are:
      
      - identityKeyBytes: the serialized form of the client's public identity key as produced by libsignal (i.e. one
        version byte followed by 32 bytes of key material for a total of 33 bytes)
      - signedEcPreKeyId: an 8-byte, big-endian representation of the ID of the client's signed EC pre-key
      - signedEcPreKeyBytes: the serialized form of the client's signed EC pre-key as produced by libsignal (i.e. one
        version byte followed by 32 bytes of key material for a total of 33 bytes)
      - lastResortKeyId: an 8-byte, big-endian representation of the ID of the client's last-resort Kyber key
      - lastResortKeyBytes: the serialized form of the client's last-resort Kyber key as produced by libsignal (i.e. one
        version byte followed by 1568 bytes of key material for a total of 1569 bytes)
      """)
  @ApiResponse(responseCode = "200", description = "Indicates that client and server have consistent views of repeated-use keys")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed")
  @ApiResponse(responseCode = "409", description = """
    Indicates that client and server have inconsistent views of repeated-use keys or one or more repeated-use keys could
    not be found
  """)
  @ApiResponse(responseCode = "422", description = "Invalid request format")
  public CompletableFuture<Response> checkKeys(
      @ReadOnly @Auth final AuthenticatedAccount auth,
      @RequestBody @NotNull @Valid final CheckKeysRequest checkKeysRequest,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    final UUID identifier = auth.getAccount().getIdentifier(checkKeysRequest.identityType());
    final byte deviceId = auth.getAuthenticatedDevice().getId();

    final CompletableFuture<Optional<ECSignedPreKey>> ecSignedPreKeyFuture =
        keysManager.getEcSignedPreKey(identifier, deviceId);

    final CompletableFuture<Optional<KEMSignedPreKey>> lastResortKeyFuture =
        keysManager.getLastResort(identifier, deviceId);

    return CompletableFuture.allOf(ecSignedPreKeyFuture, lastResortKeyFuture)
        .thenApply(ignored -> {
          final Optional<ECSignedPreKey> maybeSignedPreKey = ecSignedPreKeyFuture.join();
          final Optional<KEMSignedPreKey> maybeLastResortKey = lastResortKeyFuture.join();

          final boolean digestsMatch;

          if (maybeSignedPreKey.isPresent() && maybeLastResortKey.isPresent()) {
            final IdentityKey identityKey = auth.getAccount().getIdentityKey(checkKeysRequest.identityType());
            final ECSignedPreKey ecSignedPreKey = maybeSignedPreKey.get();
            final KEMSignedPreKey lastResortKey = maybeLastResortKey.get();

            final MessageDigest messageDigest;

            try {
              messageDigest = MessageDigest.getInstance("SHA-256");
            } catch (final NoSuchAlgorithmException e) {
              throw new AssertionError("Every implementation of the Java platform is required to support SHA-256", e);
            }

            messageDigest.update(identityKey.serialize());

            {
              final ByteBuffer ecSignedPreKeyIdBuffer = ByteBuffer.allocate(Long.BYTES);
              ecSignedPreKeyIdBuffer.putLong(ecSignedPreKey.keyId());
              ecSignedPreKeyIdBuffer.flip();

              messageDigest.update(ecSignedPreKeyIdBuffer);
              messageDigest.update(ecSignedPreKey.serializedPublicKey());
            }

            {
              final ByteBuffer lastResortKeyIdBuffer = ByteBuffer.allocate(Long.BYTES);
              lastResortKeyIdBuffer.putLong(lastResortKey.keyId());
              lastResortKeyIdBuffer.flip();

              messageDigest.update(lastResortKeyIdBuffer);
              messageDigest.update(lastResortKey.serializedPublicKey());
            }

            digestsMatch = MessageDigest.isEqual(messageDigest.digest(), checkKeysRequest.digest());
          } else {
            digestsMatch = false;
          }

          return Response.status(digestsMatch ? Response.Status.OK : Response.Status.CONFLICT).build();
        });
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
  public PreKeyResponse getDeviceKeys(
      @ReadOnly @Auth Optional<AuthenticatedAccount> auth,
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
                        Tag.of(PRIMARY_DEVICE_TAG_NAME, String.valueOf(device.isPrimary())),
                        UserAgentTagUtil.getPlatformTag(userAgent),
                        Tag.of("targetPlatform", getDevicePlatform(device).map(Enum::name).orElse("unknown")),
                        Tag.of(IDENTITY_TYPE_TAG_NAME, targetIdentifier.identityType().name()),
                        Tag.of("isStale", String.valueOf(isDeviceStale(device))),
                        Tag.of("oneTimeEcKeyAvailable", String.valueOf(unsignedEcPreKey != null))))
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
  // TODO Remove this endpoint on or after 2024-05-24
  @Deprecated(forRemoval = true)
  public CompletableFuture<Response> setSignedKey(
      @ReadOnly @Auth final AuthenticatedAccount auth,
      @Valid final ECSignedPreKey signedPreKey,
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType) {

    final UUID identifier = auth.getAccount().getIdentifier(identityType);
    final byte deviceId = auth.getAuthenticatedDevice().getId();

    if (!PreKeySignatureValidator.validatePreKeySignatures(auth.getAccount().getIdentityKey(identityType), List.of(signedPreKey))) {
      throw new WebApplicationException("Invalid signature", 422);
    }

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
