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
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.groupsend.GroupSendDerivedKeyPair;
import org.signal.libsignal.zkgroup.groupsend.GroupSendFullToken;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.GroupSendTokenHeader;
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

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Keys")
public class KeysController {

  private final RateLimiters rateLimiters;
  private final KeysManager keysManager;
  private final AccountsManager accounts;
  private final ServerSecretParams serverSecretParams;
  private final Clock clock;

  private static final String GET_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "getKeys");
  private static final String STORE_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "storeKeys");
  private static final String STORE_KEY_BUNDLE_SIZE_DISTRIBUTION_NAME =
      MetricsUtil.name(KeysController.class, "storeKeyBundleSize");
  private static final String PRIMARY_DEVICE_TAG_NAME = "isPrimary";
  private static final String IDENTITY_TYPE_TAG_NAME = "identityType";
  private static final String KEY_TYPE_TAG_NAME = "keyType";

  private static final CompletableFuture<?>[] EMPTY_FUTURE_ARRAY = new CompletableFuture[0];

  public KeysController(RateLimiters rateLimiters, KeysManager keysManager, AccountsManager accounts, ServerSecretParams serverSecretParams, Clock clock) {
    this.rateLimiters = rateLimiters;
    this.keysManager = keysManager;
    this.accounts = accounts;
    this.serverSecretParams = serverSecretParams;
    this.clock = clock;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get prekey count",
      description = "Gets the number of one-time prekeys uploaded for this device and still available")
  @ApiResponse(responseCode = "200", description = "Body contains the number of available one-time prekeys for the device.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public CompletableFuture<PreKeyCount> getStatus(@Auth final AuthenticatedDevice auth,
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType) {

    return accounts.getByAccountIdentifierAsync(auth.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount.orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          final CompletableFuture<Integer> ecCountFuture =
              keysManager.getEcCount(account.getIdentifier(identityType), auth.deviceId());

          final CompletableFuture<Integer> pqCountFuture =
              keysManager.getPqCount(account.getIdentifier(identityType), auth.deviceId());

          return ecCountFuture.thenCombine(pqCountFuture, PreKeyCount::new);
        });
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
      @Auth final AuthenticatedDevice auth,
      @RequestBody @NotNull @Valid final SetKeysRequest setKeysRequest,

      @Parameter(allowEmptyValue=true)
      @Schema(
          allowableValues={"aci", "pni"},
          defaultValue="aci",
          description="whether this operation applies to the account (aci) or phone-number (pni) identity")
      @QueryParam("identity") @DefaultValue("aci") final IdentityType identityType,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent) {

    return accounts.getByAccountIdentifierAsync(auth.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount
              .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          final Device device = account.getDevice(auth.deviceId())
              .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          final UUID identifier = account.getIdentifier(identityType);

          checkSignedPreKeySignatures(setKeysRequest, account.getIdentityKey(identityType), userAgent);

          final Tag platformTag = UserAgentTagUtil.getPlatformTag(userAgent);
          final Tag primaryDeviceTag = Tag.of(PRIMARY_DEVICE_TAG_NAME, String.valueOf(auth.deviceId() == Device.PRIMARY_ID));
          final Tag identityTypeTag = Tag.of(IDENTITY_TYPE_TAG_NAME, identityType.name());

          final List<CompletableFuture<Void>> storeFutures = new ArrayList<>(4);

          if (!setKeysRequest.preKeys().isEmpty()) {
            final Tags tags = Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "ec"));

            Metrics.counter(STORE_KEYS_COUNTER_NAME, tags).increment();

            DistributionSummary.builder(STORE_KEY_BUNDLE_SIZE_DISTRIBUTION_NAME)
                .tags(tags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry)
                .record(setKeysRequest.preKeys().size());

            storeFutures.add(keysManager.storeEcOneTimePreKeys(identifier, device.getId(), setKeysRequest.preKeys()));
          }

          if (setKeysRequest.signedPreKey() != null) {
            Metrics.counter(STORE_KEYS_COUNTER_NAME,
                    Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "ec-signed")))
                .increment();

            storeFutures.add(keysManager.storeEcSignedPreKeys(identifier, device.getId(), setKeysRequest.signedPreKey()));
          }

          if (!setKeysRequest.pqPreKeys().isEmpty()) {
            final Tags tags = Tags.of(platformTag, primaryDeviceTag, identityTypeTag, Tag.of(KEY_TYPE_TAG_NAME, "kyber"));
            Metrics.counter(STORE_KEYS_COUNTER_NAME, tags).increment();

            DistributionSummary.builder(STORE_KEY_BUNDLE_SIZE_DISTRIBUTION_NAME)
                .tags(tags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry)
                .record(setKeysRequest.pqPreKeys().size());

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
        });
  }

  private void checkSignedPreKeySignatures(final SetKeysRequest setKeysRequest,
      final IdentityKey identityKey,
      @Nullable final String userAgent) {

    final List<SignedPreKey<?>> signedPreKeys = new ArrayList<>(setKeysRequest.pqPreKeys());

    if (setKeysRequest.pqLastResortPreKey() != null) {
      signedPreKeys.add(setKeysRequest.pqLastResortPreKey());
    }

    if (setKeysRequest.signedPreKey() != null) {
      signedPreKeys.add(setKeysRequest.signedPreKey());
    }

    final boolean allSignaturesValid =
        signedPreKeys.isEmpty() || PreKeySignatureValidator.validatePreKeySignatures(identityKey, signedPreKeys, userAgent, "set-keys");

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
      @Auth final AuthenticatedDevice auth,
      @RequestBody @NotNull @Valid final CheckKeysRequest checkKeysRequest) {

    return accounts.getByAccountIdentifierAsync(auth.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount.orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          final UUID identifier = account.getIdentifier(checkKeysRequest.identityType());
          final byte deviceId = auth.deviceId();

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
                  final IdentityKey identityKey = account.getIdentityKey(checkKeysRequest.identityType());
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
        });
  }

  @GET
  @Path("/{identifier}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Fetch public keys for another user",
      description = "Retrieves the public identity key and available device prekeys for a specified account or phone-number identity")
  @ApiResponse(responseCode = "200", description = "Indicates at least one prekey was available for at least one requested device.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "A group send endorsement and other authorization (account authentication or unidentified-access key) were both provided.")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed and unidentified-access key or group send endorsement token was not supplied or invalid.")
  @ApiResponse(responseCode = "404", description = "Requested identity or device does not exist or device has no available prekeys.")
  @ApiResponse(responseCode = "429", description = "Rate limit exceeded.", headers = @Header(
      name = "Retry-After",
      description = "If present, a positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public PreKeyResponse getDeviceKeys(
      @Auth Optional<AuthenticatedDevice> maybeAuthenticatedDevice,
      @HeaderParam(HeaderUtils.UNIDENTIFIED_ACCESS_KEY) Optional<Anonymous> accessKey,
      @HeaderParam(HeaderUtils.GROUP_SEND_TOKEN) Optional<GroupSendTokenHeader> groupSendToken,

      @Parameter(description="the account or phone-number identifier to retrieve keys for")
      @PathParam("identifier") ServiceIdentifier targetIdentifier,

      @Parameter(description="the device id of a single device to retrieve prekeys for, or `*` for all enabled devices")
      @PathParam("device_id") String deviceId,

      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent)
      throws RateLimitExceededException {

    if (maybeAuthenticatedDevice.isEmpty() && accessKey.isEmpty() && groupSendToken.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    final Optional<Account> account = maybeAuthenticatedDevice
        .map(authenticatedDevice -> accounts.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
            .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED)));

    final Optional<Account> maybeTarget = accounts.getByServiceIdentifier(targetIdentifier);

    if (groupSendToken.isPresent()) {
      if (maybeAuthenticatedDevice.isPresent() || accessKey.isPresent()) {
        throw new BadRequestException();
      }
      try {
        final GroupSendFullToken token = groupSendToken.get().token();
        token.verify(List.of(targetIdentifier.toLibsignal()), clock.instant(), GroupSendDerivedKeyPair.forExpiration(token.getExpiration(), serverSecretParams));
      } catch (VerificationFailedException e) {
        throw new NotAuthorizedException(e);
      }
    } else {
      OptionalAccess.verify(account, accessKey, maybeTarget, targetIdentifier, deviceId);
    }
    final Account target = maybeTarget.orElseThrow(NotFoundException::new);

    if (account.isPresent()) {
      rateLimiters.getPreKeysLimiter().validate(
          account.get().getUuid() + "." + maybeAuthenticatedDevice.get().deviceId() + "__" + targetIdentifier.uuid()
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
                        UserAgentTagUtil.getPlatformTag(userAgent),
                        Tag.of(IDENTITY_TYPE_TAG_NAME, targetIdentifier.identityType().name()),
                        Tag.of("oneTimeEcKeyAvailable", String.valueOf(unsignedEcPreKey != null)),
                        Tag.of("pqKeyAvailable", String.valueOf(pqPreKey != null))))
                    .increment();

                if (signedEcPreKey != null || unsignedEcPreKey != null || pqPreKey != null) {
                  final int registrationId = device.getRegistrationId(targetIdentifier.identityType());

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

  private List<Device> parseDeviceId(String deviceId, Account account) {
    if (deviceId.equals("*")) {
      return account.getDevices();
    }
    try {
      byte id = Byte.parseByte(deviceId);
      return account.getDevice(id).map(List::of).orElse(List.of());
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }
}
