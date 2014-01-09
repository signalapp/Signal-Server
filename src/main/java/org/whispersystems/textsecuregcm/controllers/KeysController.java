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
import java.util.List;

@Path("/v1/keys")
public class KeysController {

  private final Logger logger = LoggerFactory.getLogger(KeysController.class);

  private final RateLimiters           rateLimiters;
  private final Keys                   keys;
  private final AccountsManager        accountsManager;
  private final FederatedClientManager federatedClientManager;

  public KeysController(RateLimiters rateLimiters, Keys keys, AccountsManager accountsManager,
                        FederatedClientManager federatedClientManager)
  {
    this.rateLimiters           = rateLimiters;
    this.keys                   = keys;
    this.accountsManager        = accountsManager;
    this.federatedClientManager = federatedClientManager;
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth Account account, @Valid PreKeyList preKeys)  {
    keys.store(account.getNumber(), account.getDeviceId(), preKeys.getLastResortKey(), preKeys.getKeys());
  }

  public List<PreKey> getKeys(Account account, String number, String relay) throws RateLimitExceededException
  {
    rateLimiters.getPreKeysLimiter().validate(account.getNumber() + "__" + number);

    try {
      UnstructuredPreKeyList keyList;

      if (relay == null) {
        keyList = keys.get(number, accountsManager.getAllByNumber(number));
      } else {
        keyList = federatedClientManager.getClient(relay).getKeys(number);
      }

      if (keyList == null || keyList.getKeys().isEmpty()) throw new WebApplicationException(Response.status(404).build());
      else                                                return keyList.getKeys();
    } catch (NoSuchPeerException e) {
      logger.info("No peer: " + relay);
      throw new WebApplicationException(Response.status(404).build());
    }
  }

  @Timed
  @GET
  @Path("/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@Auth                    Account account,
                      @PathParam("number")     String number,
                      @QueryParam("multikeys") Optional<String> multikey,
                      @QueryParam("relay")     String relay)
      throws RateLimitExceededException
  {
    if (!multikey.isPresent())
      return Response.ok(getKeys(account, number, relay).get(0)).type(MediaType.APPLICATION_JSON).build();
    else
      return Response.ok(getKeys(account, number, relay)).type(MediaType.APPLICATION_JSON).build();
  }
}
