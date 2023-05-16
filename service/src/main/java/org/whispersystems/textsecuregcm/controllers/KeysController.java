/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
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
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ChangesDeviceEnabledState;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItem;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Keys;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
@Tag(name = "Keys")
public class KeysController {

  private final RateLimiters                rateLimiters;
  private final Keys                        keys;
  private final AccountsManager             accounts;

  private static final String IDENTITY_KEY_CHANGE_COUNTER_NAME = name(KeysController.class, "identityKeyChange");
  private static final String IDENTITY_KEY_CHANGE_FORBIDDEN_COUNTER_NAME = name(KeysController.class, "identityKeyChangeForbidden");

  private static final String IDENTITY_TYPE_TAG_NAME = "identityType";
  private static final String HAS_IDENTITY_KEY_TAG_NAME = "hasIdentityKey";

  public KeysController(RateLimiters rateLimiters, Keys keys, AccountsManager accounts) {
    this.rateLimiters = rateLimiters;
    this.keys = keys;
    this.accounts = accounts;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Returns the number of available one-time prekeys for this device")
  public PreKeyCount getStatus(@Auth final AuthenticatedAccount auth,
      @QueryParam("identity") final Optional<String> identityType) {

    int ecCount = keys.getEcCount(getIdentifier(auth.getAccount(), identityType), auth.getAuthenticatedDevice().getId());
    int pqCount = keys.getPqCount(getIdentifier(auth.getAccount(), identityType), auth.getAuthenticatedDevice().getId());

    return new PreKeyCount(ecCount, pqCount);
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  @Operation(summary = "Sets the identity key for the account or phone-number identity and/or prekeys for this device")
  public void setKeys(@Auth final DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @RequestBody @NotNull @Valid final PreKeyState preKeys,

      @Parameter(allowEmptyValue=true)
      @Schema(
          allowableValues={"aci", "pni"},
          defaultValue="aci",
          description="whether this operation applies to the account (aci) or phone-number (pni) identity")
      @QueryParam("identity") final Optional<String> identityType,

      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();
    boolean updateAccount = false;

    final boolean usePhoneNumberIdentity = usePhoneNumberIdentity(identityType);

    if (preKeys.getSignedPreKey() != null &&
        !preKeys.getSignedPreKey().equals(usePhoneNumberIdentity ? device.getPhoneNumberIdentitySignedPreKey() : device.getSignedPreKey())) {
      updateAccount = true;
    }

    final String oldIdentityKey = usePhoneNumberIdentity ? account.getPhoneNumberIdentityKey() : account.getIdentityKey();
    if (!preKeys.getIdentityKey().equals(oldIdentityKey)) {
      updateAccount = true;

      final boolean hasIdentityKey = StringUtils.isNotBlank(oldIdentityKey);
      final Tags tags = Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))
          .and(HAS_IDENTITY_KEY_TAG_NAME, String.valueOf(hasIdentityKey))
          .and(IDENTITY_TYPE_TAG_NAME, usePhoneNumberIdentity ? "pni" : "aci");

      if (!device.isMaster()) {
        Metrics.counter(IDENTITY_KEY_CHANGE_FORBIDDEN_COUNTER_NAME, tags).increment();

        throw new ForbiddenException();
      }
      Metrics.counter(IDENTITY_KEY_CHANGE_COUNTER_NAME, tags).increment();
    }

    if (updateAccount) {
      account = accounts.update(account, a -> {
        if (preKeys.getSignedPreKey() != null) {
          a.getDevice(device.getId()).ifPresent(d -> {
            if (usePhoneNumberIdentity) {
              d.setPhoneNumberIdentitySignedPreKey(preKeys.getSignedPreKey());
            } else {
              d.setSignedPreKey(preKeys.getSignedPreKey());
            }
          });
        }

        if (usePhoneNumberIdentity) {
          a.setPhoneNumberIdentityKey(preKeys.getIdentityKey());
        } else {
          a.setIdentityKey(preKeys.getIdentityKey());
        }
      });
    }

    keys.store(
        getIdentifier(account, identityType), device.getId(),
        preKeys.getPreKeys(), preKeys.getPqPreKeys(), preKeys.getPqLastResortPreKey());
  }

  @Timed
  @GET
  @Path("/{identifier}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Retrieves the public identity key and available device prekeys for a specified account or phone-number identity")
  public Response getDeviceKeys(@Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,

      @Parameter(description="the account or phone-number identifier to retrieve keys for")
      @PathParam("identifier") UUID targetUuid,

      @Parameter(description="the device id of a single device to retrieve prekeys for, or `*` for all enabled devices")
      @PathParam("device_id") String deviceId,

      @Parameter(allowEmptyValue=true, description="whether to retrieve post-quantum prekeys")
      @Schema(defaultValue="false")
      @QueryParam("pq") boolean returnPqKey,

      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent)
      throws RateLimitExceededException {

    if (!auth.isPresent() && !accessKey.isPresent()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    final Optional<Account> account = auth.map(AuthenticatedAccount::getAccount);

    final Account target;
    {
      final Optional<Account> maybeTarget = accounts.getByAccountIdentifier(targetUuid)
          .or(() -> accounts.getByPhoneNumberIdentifier(targetUuid));

      OptionalAccess.verify(account, accessKey, maybeTarget, deviceId);

      target = maybeTarget.orElseThrow();
    }

    if (account.isPresent()) {
      rateLimiters.getPreKeysLimiter().validate(
          account.get().getUuid() + "." + auth.get().getAuthenticatedDevice().getId() + "__" + targetUuid
              + "." + deviceId);
    }

    final boolean usePhoneNumberIdentity = target.getPhoneNumberIdentifier().equals(targetUuid);

    List<Device> devices = parseDeviceId(deviceId, target);
    List<PreKeyResponseItem> responseItems = new ArrayList<>(devices.size());

    for (Device device : devices) {
      UUID identifier = usePhoneNumberIdentity ? target.getPhoneNumberIdentifier() : targetUuid;
      SignedPreKey signedECPreKey = usePhoneNumberIdentity ? device.getPhoneNumberIdentitySignedPreKey() : device.getSignedPreKey();
      PreKey unsignedECPreKey = keys.takeEC(identifier, device.getId()).orElse(null);
      SignedPreKey pqPreKey = returnPqKey ? keys.takePQ(identifier, device.getId()).orElse(null) : null;

      if (signedECPreKey != null || unsignedECPreKey != null || pqPreKey != null) {
        final int registrationId = usePhoneNumberIdentity ?
            device.getPhoneNumberIdentityRegistrationId().orElse(device.getRegistrationId()) :
            device.getRegistrationId();

        responseItems.add(new PreKeyResponseItem(device.getId(), registrationId, signedECPreKey, unsignedECPreKey, pqPreKey));
      }
    }

    final String identityKey = usePhoneNumberIdentity ? target.getPhoneNumberIdentityKey() : target.getIdentityKey();

    if (responseItems.isEmpty()) {
      return Response.status(404).build();
    }
    return Response.ok().entity(new PreKeyResponse(identityKey, responseItems)).build();
  }

  @Timed
  @PUT
  @Path("/signed")
  @Consumes(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setSignedKey(@Auth final AuthenticatedAccount auth,
      @Valid final SignedPreKey signedPreKey,
      @QueryParam("identity") final Optional<String> identityType) {

    Device device = auth.getAuthenticatedDevice();

    accounts.updateDevice(auth.getAccount(), device.getId(), d -> {
      if (usePhoneNumberIdentity(identityType)) {
        d.setPhoneNumberIdentitySignedPreKey(signedPreKey);
      } else {
        d.setSignedPreKey(signedPreKey);
      }
    });
  }

  @Timed
  @GET
  @Path("/signed")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<SignedPreKey> getSignedKey(@Auth final AuthenticatedAccount auth,
      @QueryParam("identity") final Optional<String> identityType) {

    Device device = auth.getAuthenticatedDevice();
    SignedPreKey signedPreKey = usePhoneNumberIdentity(identityType) ?
        device.getPhoneNumberIdentitySignedPreKey() : device.getSignedPreKey();

    return Optional.ofNullable(signedPreKey);
  }

  private static boolean usePhoneNumberIdentity(final Optional<String> identityType) {
    return "pni".equals(identityType.map(String::toLowerCase).orElse("aci"));
  }

  private static UUID getIdentifier(final Account account, final Optional<String> identityType) {
    return usePhoneNumberIdentity(identityType) ?
        account.getPhoneNumberIdentifier() :
        account.getUuid();
  }

  private List<Device> parseDeviceId(String deviceId, Account account) {
    if (deviceId.equals("*")) {
      return account.getDevices().stream().filter(Device::isEnabled).toList();
    }
    try {
      long id = Long.parseLong(deviceId);
      return account.getDevice(id).filter(Device::isEnabled).map(List::of).orElse(List.of());
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }
}
