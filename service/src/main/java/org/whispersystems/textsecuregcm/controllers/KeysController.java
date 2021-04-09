/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItem;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v2/keys")
public class KeysController {

  private final RateLimiters                rateLimiters;
  private final KeysDynamoDb                keysDynamoDb;
  private final AccountsManager             accounts;
  private final DirectoryQueue              directoryQueue;

  private static final String INTERNATIONAL_PREKEY_REQUEST_COUNTER_NAME =
      name(KeysController.class, "internationalPreKeyGet");

  private static final String SOURCE_COUNTRY_TAG_NAME = "sourceCountry";
  private static final String PREKEY_TARGET_IDENTIFIER_TAG_NAME =  "identifierType";

  public KeysController(RateLimiters rateLimiters, KeysDynamoDb keysDynamoDb, AccountsManager accounts, DirectoryQueue directoryQueue) {
    this.rateLimiters                = rateLimiters;
    this.keysDynamoDb                = keysDynamoDb;
    this.accounts                    = accounts;
    this.directoryQueue              = directoryQueue;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public PreKeyCount getStatus(@Auth Account account) {
    int count = keysDynamoDb.getCount(account, account.getAuthenticatedDevice().get().getId());

    if (count > 0) {
      count = count - 1;
    }

    return new PreKeyCount(count);
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth DisabledPermittedAccount disabledPermittedAccount, @Valid PreKeyState preKeys)  {
    Account account           = disabledPermittedAccount.getAccount();
    Device  device            = account.getAuthenticatedDevice().get();
    boolean wasAccountEnabled = account.isEnabled();
    boolean updateAccount     = false;

    if (!preKeys.getSignedPreKey().equals(device.getSignedPreKey())) {
      device.setSignedPreKey(preKeys.getSignedPreKey());
      updateAccount = true;
    }

    if (!preKeys.getIdentityKey().equals(account.getIdentityKey())) {
      account.setIdentityKey(preKeys.getIdentityKey());
      updateAccount = true;
    }

    if (updateAccount) {
      accounts.update(account);

      if (!wasAccountEnabled && account.isEnabled()) {
        directoryQueue.refreshRegisteredUser(account);
      }
    }

    keysDynamoDb.store(account, device.getId(), preKeys.getPreKeys());
  }

  @Timed
  @GET
  @Path("/{identifier}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyResponse> getDeviceKeys(@Auth                                     Optional<Account> account,
                                                @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
                                                @PathParam("identifier")                  AmbiguousIdentifier targetName,
                                                @PathParam("device_id")                   String deviceId)
      throws RateLimitExceededException
  {
    if (!account.isPresent() && !accessKey.isPresent()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    Optional<Account> target = accounts.get(targetName);
    OptionalAccess.verify(account, accessKey, target, deviceId);

    assert(target.isPresent());

    if (account.isPresent()) {
      rateLimiters.getPreKeysLimiter().validate(account.get().getNumber() + "." + account.get().getAuthenticatedDevice().get().getId() +  "__" + target.get().getNumber() + "." + deviceId);

      final String accountCountryCode = Util.getCountryCode(account.get().getNumber());
      final String targetCountryCode = Util.getCountryCode(target.get().getNumber());

      if (!accountCountryCode.equals(targetCountryCode)) {
        final Tags tags = Tags.of(SOURCE_COUNTRY_TAG_NAME, accountCountryCode)
            .and(PREKEY_TARGET_IDENTIFIER_TAG_NAME, targetName.hasNumber() ? "number" : "uuid");
        Metrics.counter(INTERNATIONAL_PREKEY_REQUEST_COUNTER_NAME, tags)
            .increment();
      }
    }

    Map<Long, PreKey>        preKeysByDeviceId = getLocalKeys(target.get(), deviceId);
    List<PreKeyResponseItem> responseItems     = new LinkedList<>();

    for (Device device : target.get().getDevices()) {
      if (device.isEnabled() && (deviceId.equals("*") || device.getId() == Long.parseLong(deviceId))) {
        SignedPreKey signedPreKey = device.getSignedPreKey();
        PreKey       preKey       = preKeysByDeviceId.get(device.getId());

        if (signedPreKey != null || preKey != null) {
          responseItems.add(new PreKeyResponseItem(device.getId(), device.getRegistrationId(), signedPreKey, preKey));
        }
      }
    }

    if (responseItems.isEmpty()) return Optional.empty();
    else                         return Optional.of(new PreKeyResponse(target.get().getIdentityKey(), responseItems));
  }

  @Timed
  @PUT
  @Path("/signed")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setSignedKey(@Auth Account account, @Valid SignedPreKey signedPreKey) {
    Device  device            = account.getAuthenticatedDevice().get();
    boolean wasAccountEnabled = account.isEnabled();

    device.setSignedPreKey(signedPreKey);
    accounts.update(account);

    if (!wasAccountEnabled && account.isEnabled()) {
      directoryQueue.refreshRegisteredUser(account);
    }
  }

  @Timed
  @GET
  @Path("/signed")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<SignedPreKey> getSignedKey(@Auth Account account) {
    Device       device       = account.getAuthenticatedDevice().get();
    SignedPreKey signedPreKey = device.getSignedPreKey();

    if (signedPreKey != null) return Optional.of(signedPreKey);
    else                      return Optional.empty();
  }

  private Map<Long, PreKey> getLocalKeys(Account destination, String deviceIdSelector) {
    try {
      if (deviceIdSelector.equals("*")) {
        return keysDynamoDb.take(destination);
      }

      long deviceId = Long.parseLong(deviceIdSelector);

      return keysDynamoDb.take(destination, deviceId)
              .map(preKey -> Map.of(deviceId, preKey))
              .orElse(Collections.emptyMap());
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }
}
