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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV1;
import org.whispersystems.textsecuregcm.entities.PreKeyStateV1;
import org.whispersystems.textsecuregcm.entities.PreKeyV1;
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

@Path("/v1/keys")
public class KeysControllerV1 extends KeysController {

  private final Logger logger = LoggerFactory.getLogger(KeysControllerV1.class);

  public KeysControllerV1(RateLimiters rateLimiters, Keys keys, AccountsManager accounts,
                          FederatedClientManager federatedClientManager)
  {
    super(rateLimiters, keys, accounts, federatedClientManager);
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth Account account, @Valid PreKeyStateV1 preKeys)  {
    Device device      = account.getAuthenticatedDevice().get();
    String identityKey = preKeys.getLastResortKey().getIdentityKey();

    if (!identityKey.equals(account.getIdentityKey())) {
      account.setIdentityKey(identityKey);
      accounts.update(account);
    }

    keys.store(account.getNumber(), device.getId(), preKeys.getKeys(), preKeys.getLastResortKey());
  }

  @Timed
  @GET
  @Path("/{number}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyResponseV1> getDeviceKey(@Auth                   Account account,
                                                 @PathParam("number")    String number,
                                                 @PathParam("device_id") String deviceId,
                                                 @QueryParam("relay")    Optional<String> relay)
      throws RateLimitExceededException
  {
    try {
      if (account.isRateLimited()) {
        rateLimiters.getPreKeysLimiter().validate(account.getNumber() +  "__" + number + "." + deviceId);
      }

      if (relay.isPresent()) {
        return federatedClientManager.getClient(relay.get()).getKeysV1(number, deviceId);
      }

      TargetKeys targetKeys = getLocalKeys(number, deviceId);

      if (!targetKeys.getKeys().isPresent()) {
        return Optional.absent();
      }

      List<PreKeyV1> preKeys     = new LinkedList<>();
      Account        destination = targetKeys.getDestination();

      for (KeyRecord record : targetKeys.getKeys().get()) {
        Optional<Device> device = destination.getDevice(record.getDeviceId());
        if (device.isPresent() && device.get().isActive()) {
          preKeys.add(new PreKeyV1(record.getDeviceId(), record.getKeyId(),
                                   record.getPublicKey(), destination.getIdentityKey(),
                                   device.get().getRegistrationId()));
        }
      }

      if (preKeys.isEmpty()) return Optional.absent();
      else                   return Optional.of(new PreKeyResponseV1(preKeys));
    } catch (NoSuchPeerException | NoSuchUserException e) {
      throw new WebApplicationException(Response.status(404).build());
    }
  }

  @Timed
  @GET
  @Path("/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyV1> get(@Auth                Account account,
                                @PathParam("number") String number,
                                @QueryParam("relay") Optional<String> relay)
      throws RateLimitExceededException
  {
    Optional<PreKeyResponseV1> results = getDeviceKey(account, number, String.valueOf(Device.MASTER_ID), relay);

    if (results.isPresent()) return Optional.of(results.get().getKeys().get(0));
    else                     return Optional.absent();
  }

}
