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

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.AccountCount;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.ClientContacts;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV2;
import org.whispersystems.textsecuregcm.entities.PreKeyV1;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV1;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.federation.NonLimitedAccount;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Util;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.auth.Auth;

@Path("/")
public class FederationController {

  private final Logger logger = LoggerFactory.getLogger(FederationController.class);

  private static final int ACCOUNT_CHUNK_SIZE = 10000;

  private final AccountsManager      accounts;
  private final AttachmentController attachmentController;
  private final KeysControllerV1     keysControllerV1;
  private final KeysControllerV2     keysControllerV2;
  private final MessageController    messageController;

  public FederationController(AccountsManager      accounts,
                              AttachmentController attachmentController,
                              KeysControllerV1     keysControllerV1,
                              KeysControllerV2     keysControllerV2,
                              MessageController    messageController)
  {
    this.accounts             = accounts;
    this.attachmentController = attachmentController;
    this.keysControllerV1     = keysControllerV1;
    this.keysControllerV2     = keysControllerV2;
    this.messageController    = messageController;
  }

  @Timed
  @GET
  @Path("/v1/federation/attachment/{attachmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public AttachmentUri getSignedAttachmentUri(@Auth                      FederatedPeer peer,
                                              @PathParam("attachmentId") long attachmentId)
      throws IOException
  {
    return attachmentController.redirectToAttachment(new NonLimitedAccount("Unknown", -1, peer.getName()),
                                                     attachmentId, Optional.<String>absent());
  }

  @Timed
  @GET
  @Path("/v1/federation/key/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyV1> getKey(@Auth                FederatedPeer peer,
                                   @PathParam("number") String number)
      throws IOException
  {
    try {
      return keysControllerV1.get(new NonLimitedAccount("Unknown", -1, peer.getName()),
                                  number, Optional.<String>absent());
    } catch (RateLimitExceededException e) {
      logger.warn("Rate limiting on federated channel", e);
      throw new IOException(e);
    }
  }

  @Timed
  @GET
  @Path("/v1/federation/key/{number}/{device}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyResponseV1> getKeysV1(@Auth                FederatedPeer peer,
                                              @PathParam("number") String number,
                                              @PathParam("device") String device)
      throws IOException
  {
    try {
      return keysControllerV1.getDeviceKey(new NonLimitedAccount("Unknown", -1, peer.getName()),
                                           number, device, Optional.<String>absent());
    } catch (RateLimitExceededException e) {
      logger.warn("Rate limiting on federated channel", e);
      throw new IOException(e);
    }
  }

  @Timed
  @GET
  @Path("/v2/federation/key/{number}/{device}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyResponseV2> getKeysV2(@Auth                FederatedPeer peer,
                                              @PathParam("number") String number,
                                              @PathParam("device") String device)
      throws IOException
  {
    try {
      return keysControllerV2.getDeviceKey(new NonLimitedAccount("Unknown", -1, peer.getName()),
                                           number, device, Optional.<String>absent());
    } catch (RateLimitExceededException e) {
      logger.warn("Rate limiting on federated channel", e);
      throw new IOException(e);
    }
  }

  @Timed
  @PUT
  @Path("/v1/federation/messages/{source}/{sourceDeviceId}/{destination}")
  public void sendMessages(@Auth                        FederatedPeer peer,
                           @PathParam("source")         String source,
                           @PathParam("sourceDeviceId") long sourceDeviceId,
                           @PathParam("destination")    String destination,
                           @Valid                       IncomingMessageList messages)
      throws IOException
  {
    try {
      messages.setRelay(null);
      messageController.sendMessage(new NonLimitedAccount(source, sourceDeviceId, peer.getName()), destination, messages);
    } catch (RateLimitExceededException e) {
      logger.warn("Rate limiting on federated channel", e);
      throw new IOException(e);
    }
  }

  @Timed
  @GET
  @Path("/v1/federation/user_count")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountCount getUserCount(@Auth FederatedPeer peer) {
    return new AccountCount((int)accounts.getCount());
  }

  @Timed
  @GET
  @Path("/v1/federation/user_tokens/{offset}")
  @Produces(MediaType.APPLICATION_JSON)
  public ClientContacts getUserTokens(@Auth                FederatedPeer peer,
                                      @PathParam("offset") int offset)
  {
    List<Account>       accountList    = accounts.getAll(offset, ACCOUNT_CHUNK_SIZE);
    List<ClientContact> clientContacts = new LinkedList<>();

    for (Account account : accountList) {
      byte[]        token         = Util.getContactToken(account.getNumber());
      ClientContact clientContact = new ClientContact(token, null, account.getSupportsSms());

      if (!account.isActive()) {
        clientContact.setInactive(true);
      }

      clientContacts.add(clientContact);
    }

    return new ClientContacts(clientContacts);
  }
}
