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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
@Tag(name = "Keys")
public class KeysController {

  private final RateLimiters rateLimiters;
  private final KeysManager keys;
  private final AccountsManager accounts;

  private static final String GET_KEYS_COUNTER_NAME = MetricsUtil.name(KeysController.class, "getKeys");

  private static final CompletableFuture<?>[] EMPTY_FUTURE_ARRAY = new CompletableFuture[0];

  public KeysController(RateLimiters rateLimiters, KeysManager keys, AccountsManager accounts) {
    this.rateLimiters = rateLimiters;
    this.keys = keys;
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
        keys.getEcCount(auth.getAccount().getIdentifier(identityType), auth.getAuthenticatedDevice().getId());

    final CompletableFuture<Integer> pqCountFuture =
        keys.getPqCount(auth.getAccount().getIdentifier(identityType), auth.getAuthenticatedDevice().getId());

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

    final CompletableFuture<Account> updateAccountFuture;

    if (setKeysRequest.signedPreKey() != null &&
        !setKeysRequest.signedPreKey().equals(device.getSignedPreKey(identityType))) {

      updateAccountFuture = accounts.updateDeviceTransactionallyAsync(account,
              device.getId(),
              d -> {
                switch (identityType) {
                  case ACI -> d.setSignedPreKey(setKeysRequest.signedPreKey());
                  case PNI -> d.setPhoneNumberIdentitySignedPreKey(setKeysRequest.signedPreKey());
                }
              },
              d -> List.of(keys.buildWriteItemForEcSignedPreKey(identifier, d.getId(), setKeysRequest.signedPreKey())))
          .toCompletableFuture();
    } else {
      updateAccountFuture = CompletableFuture.completedFuture(account);
    }

    return updateAccountFuture.thenCompose(updatedAccount -> {
          final List<CompletableFuture<Void>> storeFutures = new ArrayList<>(3);

          if (setKeysRequest.preKeys() != null && !setKeysRequest.preKeys().isEmpty()) {
            storeFutures.add(keys.storeEcOneTimePreKeys(identifier, device.getId(), setKeysRequest.preKeys()));
          }

          if (setKeysRequest.pqPreKeys() != null && !setKeysRequest.pqPreKeys().isEmpty()) {
            storeFutures.add(keys.storeKemOneTimePreKeys(identifier, device.getId(), setKeysRequest.pqPreKeys()));
          }

          if (setKeysRequest.pqLastResortPreKey() != null) {
            storeFutures.add(
                keys.storePqLastResort(identifier, device.getId(), setKeysRequest.pqLastResortPreKey()));
          }

          return CompletableFuture.allOf(storeFutures.toArray(EMPTY_FUTURE_ARRAY));
        })
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

      @Parameter(allowEmptyValue=true, description="whether to retrieve post-quantum prekeys")
      @Schema(defaultValue="false")
      @QueryParam("pq") boolean returnPqKey,

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

    Metrics.counter(GET_KEYS_COUNTER_NAME, Tags.of(
            UserAgentTagUtil.getPlatformTag(userAgent),
            io.micrometer.core.instrument.Tag.of("wildcardDeviceId", String.valueOf("*".equals(deviceId)))))
        .increment();

    final List<PreKeyResponseItem> responseItems = Flux.fromIterable(parseDeviceId(deviceId, target))
        .flatMap(device -> Mono.zip(
            Mono.just(device),
            Mono.fromFuture(() -> keys.getEcSignedPreKey(targetIdentifier.uuid(), device.getId())),
            Mono.fromFuture(() -> keys.takeEC(targetIdentifier.uuid(), device.getId())),
            Mono.fromFuture(() -> returnPqKey ? keys.takePQ(targetIdentifier.uuid(), device.getId())
                : CompletableFuture.<Optional<KEMSignedPreKey>>completedFuture(Optional.empty()))
        )).filter(keys -> keys.getT2().isPresent() || keys.getT3().isPresent() || keys.getT4().isPresent())
        .map(deviceAndKeys -> {
          final Device device = deviceAndKeys.getT1();
          final int registrationId = switch (targetIdentifier.identityType()) {
            case ACI -> device.getRegistrationId();
            case PNI -> device.getPhoneNumberIdentityRegistrationId().orElse(device.getRegistrationId());
          };
          return new PreKeyResponseItem(device.getId(), registrationId,
              deviceAndKeys.getT2().orElse(null),
              deviceAndKeys.getT3().orElse(null),
              deviceAndKeys.getT4().orElse(null));
        }).collectList()
        .timeout(Duration.ofSeconds(30))
        .blockOptional()
        .orElse(Collections.emptyList());

    final IdentityKey identityKey = target.getIdentityKey(targetIdentifier.identityType());

    if (responseItems.isEmpty()) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    }

    return new PreKeyResponse(identityKey, responseItems);
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

    return accounts.updateDeviceTransactionallyAsync(auth.getAccount(),
            deviceId,
            d -> {
              switch (identityType) {
                case ACI -> d.setSignedPreKey(signedPreKey);
                case PNI -> d.setPhoneNumberIdentitySignedPreKey(signedPreKey);
              }
            },
            d -> List.of(keys.buildWriteItemForEcSignedPreKey(identifier, d.getId(), signedPreKey)))
        .toCompletableFuture()
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
