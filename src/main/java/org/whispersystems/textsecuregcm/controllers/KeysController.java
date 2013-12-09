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

import com.yammer.dropwizard.auth.Auth;
import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.PreKeyList;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
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

@Path("/v1/keys")
public class KeysController {

  private final Logger logger = LoggerFactory.getLogger(AccountController.class);

  private final RateLimiters           rateLimiters;
  private final Keys                   keys;
  private final FederatedClientManager federatedClientManager;

  public KeysController(RateLimiters rateLimiters, Keys keys,
                        FederatedClientManager federatedClientManager)
  {
    this.rateLimiters           = rateLimiters;
    this.keys                   = keys;
    this.federatedClientManager = federatedClientManager;
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth Account account, @Valid PreKeyList preKeys)  {
    keys.store(account.getNumber(), preKeys.getLastResortKey(), preKeys.getKeys());
  }

  @Timed
  @GET
  @Path("/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public PreKey get(@Auth                Account account,
                    @PathParam("number") String number,
                    @QueryParam("relay") String relay)
      throws RateLimitExceededException
  {
    rateLimiters.getPreKeysLimiter().validate(account.getNumber() + "__" + number);

    try {
      PreKey key;

      if (relay == null) key = keys.get(number);
      else               key = federatedClientManager.getClient(relay).getKey(number);

      if (key == null) throw new WebApplicationException(Response.status(404).build());
      else             return key;
    } catch (NoSuchPeerException e) {
      logger.info("No peer: " + relay);
      throw new WebApplicationException(Response.status(404).build());
    }
  }

}
