/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
public class KeysController {

  private final RateLimiters                rateLimiters;
  private final Keys                        keys;
  private final AccountsManager             accounts;

  private static final String PREKEY_REQUEST_COUNTER_NAME = name(KeysController.class, "preKeyGet");

  private static final String SOURCE_COUNTRY_TAG_NAME = "sourceCountry";
  private static final String INTERNATIONAL_TAG_NAME = "international";

  public KeysController(RateLimiters rateLimiters, Keys keys, AccountsManager accounts) {
    this.rateLimiters = rateLimiters;
    this.keys = keys;
    this.accounts = accounts;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public PreKeyCount getStatus(@Auth final AuthenticatedAccount auth,
      @QueryParam("identity") final Optional<String> identityType) {

    int count = keys.getCount(getIdentifier(auth.getAccount(), identityType), auth.getAuthenticatedDevice().getId());

    if (count > 0) {
      count = count - 1;
    }

    return new PreKeyCount(count);
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setKeys(@Auth final DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @NotNull @Valid final PreKeyState preKeys,
      @QueryParam("identity") final Optional<String> identityType) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();
    boolean updateAccount = false;

    final boolean usePhoneNumberIdentity = usePhoneNumberIdentity(identityType);

    if (!preKeys.getSignedPreKey().equals(usePhoneNumberIdentity ? device.getPhoneNumberIdentitySignedPreKey() : device.getSignedPreKey())) {
      updateAccount = true;
    }

    if (!preKeys.getIdentityKey().equals(usePhoneNumberIdentity ? account.getPhoneNumberIdentityKey() : account.getIdentityKey())) {
      updateAccount = true;
      if (!device.isMaster()) {
        throw new ForbiddenException();
      }
    }

    if (updateAccount) {
      account = accounts.update(account, a -> {
        a.getDevice(device.getId()).ifPresent(d -> {
          if (usePhoneNumberIdentity) {
            d.setPhoneNumberIdentitySignedPreKey(preKeys.getSignedPreKey());
          } else {
            d.setSignedPreKey(preKeys.getSignedPreKey());
          }
        });

        if (usePhoneNumberIdentity) {
          a.setPhoneNumberIdentityKey(preKeys.getIdentityKey());
        } else {
          a.setIdentityKey(preKeys.getIdentityKey());
        }
      });
    }

    keys.store(getIdentifier(account, identityType), device.getId(), preKeys.getPreKeys());
  }

  @Timed
  @GET
  @Path("/{identifier}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDeviceKeys(@Auth Optional<AuthenticatedAccount> auth,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @PathParam("identifier") UUID targetUuid,
      @PathParam("device_id") String deviceId,
      @HeaderParam("User-Agent") String userAgent)
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

    {
      final String sourceCountryCode = account.map(a -> Util.getCountryCode(a.getNumber())).orElse("0");
      final String targetCountryCode = Util.getCountryCode(target.getNumber());

      Metrics.counter(PREKEY_REQUEST_COUNTER_NAME, Tags.of(
          SOURCE_COUNTRY_TAG_NAME, sourceCountryCode,
          INTERNATIONAL_TAG_NAME, String.valueOf(!sourceCountryCode.equals(targetCountryCode))
      )).increment();
    }

    if (account.isPresent()) {
      rateLimiters.getPreKeysLimiter().validate(
          account.get().getUuid() + "." + auth.get().getAuthenticatedDevice().getId() + "__" + targetUuid
              + "." + deviceId);
    }

    final boolean usePhoneNumberIdentity = target.getPhoneNumberIdentifier().equals(targetUuid);

    Map<Long, PreKey>        preKeysByDeviceId = getLocalKeys(target, deviceId, usePhoneNumberIdentity);
    List<PreKeyResponseItem> responseItems     = new LinkedList<>();

    for (Device device : target.getDevices()) {
      if (device.isEnabled() && (deviceId.equals("*") || device.getId() == Long.parseLong(deviceId))) {
        SignedPreKey signedPreKey = usePhoneNumberIdentity ? device.getPhoneNumberIdentitySignedPreKey() : device.getSignedPreKey();
        PreKey       preKey       = preKeysByDeviceId.get(device.getId());

        if (signedPreKey != null || preKey != null) {
          responseItems.add(new PreKeyResponseItem(device.getId(), device.getRegistrationId(), signedPreKey, preKey));
        }
      }
    }

    final String identityKey = usePhoneNumberIdentity ? target.getPhoneNumberIdentityKey() : target.getIdentityKey();

    if (responseItems.isEmpty()) return Response.status(404).build();
    else                         return Response.ok().entity(new PreKeyResponse(identityKey, responseItems)).build();
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

  private Map<Long, PreKey> getLocalKeys(Account destination, String deviceIdSelector, final boolean usePhoneNumberIdentity) {
    final Map<Long, PreKey> preKeys;

    final UUID identifier = usePhoneNumberIdentity ?
        destination.getPhoneNumberIdentifier() :
        destination.getUuid();

    if (deviceIdSelector.equals("*")) {
      preKeys = new HashMap<>();

      for (final Device device : destination.getDevices()) {
        keys.take(identifier, device.getId()).ifPresent(preKey -> preKeys.put(device.getId(), preKey));
      }
    } else {
      try {
        long deviceId = Long.parseLong(deviceIdSelector);

        preKeys = keys.take(identifier, deviceId)
            .map(preKey -> Map.of(deviceId, preKey))
            .orElse(Collections.emptyMap());
      } catch (NumberFormatException e) {
        throw new WebApplicationException(Response.status(422).build());
      }
    }

    return preKeys;
  }
}
