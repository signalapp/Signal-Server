/**
 * Copyright (C) 2014 Open Whisper Systems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PreKeyCount;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItem;
import org.whispersystems.textsecuregcm.entities.PreKeyResponse;
import org.whispersystems.textsecuregcm.entities.PreKeyState;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeyRecord;
import org.whispersystems.textsecuregcm.storage.Keys;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.auth.Auth;

@Path("/v2/keys")
public class KeysController {

  private static final Logger logger = LoggerFactory.getLogger(KeysController.class);

  private final RateLimiters           rateLimiters;
  private final Keys                   keys;
  private final AccountsManager        accounts;
  private final FederatedClientManager federatedClientManager;

  public KeysController(RateLimiters rateLimiters, Keys keys, AccountsManager accounts,
                        FederatedClientManager federatedClientManager)
  {
    this.rateLimiters           = rateLimiters;
    this.keys                   = keys;
    this.accounts               = accounts;
    this.federatedClientManager = federatedClientManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public PreKeyCount getStatus(@Auth Account account) {
    int count = keys.getCount(account.getNumber(), account.getAuthenticatedDevice().get().getId());

    if (count > 0) {
      count = count - 1;
    }

    return new PreKeyCount(count);
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth Account account, @Valid PreKeyState preKeys)  {
    Device  device        = account.getAuthenticatedDevice().get();
    boolean updateAccount = false;

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
    }

    keys.store(account.getNumber(), device.getId(), preKeys.getPreKeys());
  }

  @Timed
  @GET
  @Path("/{number}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyResponse> getDeviceKeys(@Auth                   Account account,
                                                @PathParam("number")    String number,
                                                @PathParam("device_id") String deviceId,
                                                @QueryParam("relay")    Optional<String> relay)
      throws RateLimitExceededException
  {
    try {
      if (relay.isPresent()) {
        return federatedClientManager.getClient(relay.get()).getKeysV2(number, deviceId);
      }

      Account target = getAccount(number, deviceId);

      if (account.isRateLimited()) {
        rateLimiters.getPreKeysLimiter().validate(account.getNumber() +  "__" + number + "." + deviceId);
      }

      Optional<List<KeyRecord>> targetKeys = getLocalKeys(target, deviceId);
      List<PreKeyResponseItem>  devices    = new LinkedList<>();

      for (Device device : target.getDevices()) {
        if (device.isActive() && (deviceId.equals("*") || device.getId() == Long.parseLong(deviceId))) {
          SignedPreKey signedPreKey = device.getSignedPreKey();
          PreKey preKey       = null;

          if (targetKeys.isPresent()) {
            for (KeyRecord keyRecord : targetKeys.get()) {
              if (!keyRecord.isLastResort() && keyRecord.getDeviceId() == device.getId()) {
                preKey = new PreKey(keyRecord.getKeyId(), keyRecord.getPublicKey());
              }
            }
          }

          if (signedPreKey != null || preKey != null) {
            devices.add(new PreKeyResponseItem(device.getId(), device.getRegistrationId(), signedPreKey, preKey));
          }
        }
      }

      if (devices.isEmpty()) return Optional.absent();
      else                   return Optional.of(new PreKeyResponse(target.getIdentityKey(), devices));
    } catch (NoSuchPeerException | NoSuchUserException e) {
      throw new WebApplicationException(Response.status(404).build());
    }
  }

  @Timed
  @PUT
  @Path("/signed")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setSignedKey(@Auth Account account, @Valid SignedPreKey signedPreKey) {
    Device device = account.getAuthenticatedDevice().get();
    device.setSignedPreKey(signedPreKey);
    accounts.update(account);
  }

  @Timed
  @GET
  @Path("/signed")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<SignedPreKey> getSignedKey(@Auth Account account) {
    Device       device       = account.getAuthenticatedDevice().get();
    SignedPreKey signedPreKey = device.getSignedPreKey();

    if (signedPreKey != null) return Optional.of(signedPreKey);
    else                      return Optional.absent();
  }

  private Optional<List<KeyRecord>> getLocalKeys(Account destination, String deviceIdSelector)
      throws NoSuchUserException
  {
    try {
      if (deviceIdSelector.equals("*")) {
        return keys.get(destination.getNumber());
      }

      long deviceId = Long.parseLong(deviceIdSelector);

      for (int i=0;i<20;i++) {
        try {
          return keys.get(destination.getNumber(), deviceId);
        } catch (UnableToExecuteStatementException e) {
          logger.info(e.getMessage());
        }
      }

      throw new WebApplicationException(Response.status(500).build());
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }

  private Account getAccount(String number, String deviceSelector)
      throws NoSuchUserException
  {
    try {
      Optional<Account> account = accounts.get(number);

      if (!account.isPresent() || !account.get().isActive()) {
        throw new NoSuchUserException("No active account");
      }

      if (!deviceSelector.equals("*")) {
        long deviceId = Long.parseLong(deviceSelector);

        Optional<Device> targetDevice = account.get().getDevice(deviceId);

        if (!targetDevice.isPresent() || !targetDevice.get().isActive()) {
          throw new NoSuchUserException("No active device");
        }
      }

      return account.get();
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }
}
