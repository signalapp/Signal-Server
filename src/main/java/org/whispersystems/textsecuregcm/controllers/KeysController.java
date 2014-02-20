/**
 * Copyright (C) 2013 Open WhisperSystems
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

import com.google.common.base.Optional;
import com.yammer.dropwizard.auth.Auth;
import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyList;
import org.whispersystems.textsecuregcm.entities.UnstructuredPreKeyList;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
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

@Path("/v1/keys")
public class KeysController {

  private final Logger logger = LoggerFactory.getLogger(KeysController.class);

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

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth Account account, @Valid PreKeyList preKeys)  {
    Device device = account.getAuthenticatedDevice().get();
    keys.store(account.getNumber(), device.getId(), preKeys.getKeys(), preKeys.getLastResortKey());
  }

  @Timed
  @GET
  @Path("/{number}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public UnstructuredPreKeyList getDeviceKey(@Auth                   Account account,
                                             @PathParam("number")    String number,
                                             @PathParam("device_id") String deviceId,
                                             @QueryParam("relay")    Optional<String> relay)
      throws RateLimitExceededException
  {
    try {
      if (account.isRateLimited()) {
        rateLimiters.getPreKeysLimiter().validate(account.getNumber() +  "__" + number + "." + deviceId);
      }

      Optional<UnstructuredPreKeyList> results;

      if (!relay.isPresent()) results = getLocalKeys(number, deviceId);
      else                    results = federatedClientManager.getClient(relay.get()).getKeys(number, deviceId);

      if (results.isPresent()) return results.get();
      else                     throw new WebApplicationException(Response.status(404).build());
    } catch (NoSuchPeerException e) {
      throw new WebApplicationException(Response.status(404).build());
    }
  }

  @Timed
  @GET
  @Path("/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public PreKey get(@Auth                Account account,
                    @PathParam("number") String number,
                    @QueryParam("relay") Optional<String> relay)
      throws RateLimitExceededException
  {
    UnstructuredPreKeyList results = getDeviceKey(account, number, String.valueOf(Device.MASTER_ID), relay);
    return results.getKeys().get(0);
  }

  private Optional<UnstructuredPreKeyList> getLocalKeys(String number, String deviceIdSelector) {
    Optional<Account> destination = accounts.get(number);

    if (!destination.isPresent() || !destination.get().isActive()) {
      return Optional.absent();
    }

    try {
      if (deviceIdSelector.equals("*")) {
        Optional<UnstructuredPreKeyList> preKeys = keys.get(number);
        return getActiveKeys(destination.get(), preKeys);
      }

      long             deviceId     = Long.parseLong(deviceIdSelector);
      Optional<Device> targetDevice = destination.get().getDevice(deviceId);

      if (!targetDevice.isPresent() || !targetDevice.get().isActive()) {
        return Optional.absent();
      }

      Optional<UnstructuredPreKeyList> preKeys = keys.get(number, deviceId);
      return getActiveKeys(destination.get(), preKeys);
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }

  private Optional<UnstructuredPreKeyList> getActiveKeys(Account destination,
                                                         Optional<UnstructuredPreKeyList> preKeys)
  {
    if (!preKeys.isPresent()) return Optional.absent();

    List<PreKey> filteredKeys = new LinkedList<>();

    for (PreKey preKey : preKeys.get().getKeys()) {
      Optional<Device> device = destination.getDevice(preKey.getDeviceId());

      if (device.isPresent() && device.get().isActive()) {
        preKey.setRegistrationId(device.get().getRegistrationId());
        filteredKeys.add(preKey);
      }
    }

    if (filteredKeys.isEmpty()) return Optional.absent();
    else                        return Optional.of(new UnstructuredPreKeyList(filteredKeys));
  }
}
