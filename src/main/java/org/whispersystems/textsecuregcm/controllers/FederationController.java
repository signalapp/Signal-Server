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

import com.amazonaws.HttpMethod;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yammer.dropwizard.auth.Auth;
import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.AccountCount;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.ClientContacts;
import org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;
import org.whispersystems.textsecuregcm.entities.MessageResponse;
import org.whispersystems.textsecuregcm.entities.RelayMessage;
import org.whispersystems.textsecuregcm.entities.UnstructuredPreKeyList;
import org.whispersystems.textsecuregcm.federation.FederatedPeer;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.UrlSigner;
import org.whispersystems.textsecuregcm.util.Util;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/v1/federation")
public class FederationController {

  private final Logger logger = LoggerFactory.getLogger(FederationController.class);

  private static final int ACCOUNT_CHUNK_SIZE = 10000;

  private final PushSender      pushSender;
  private final Keys            keys;
  private final AccountsManager accounts;
  private final UrlSigner       urlSigner;

  public FederationController(Keys keys, AccountsManager accounts, PushSender pushSender, UrlSigner urlSigner) {
    this.keys       = keys;
    this.accounts   = accounts;
    this.pushSender = pushSender;
    this.urlSigner  = urlSigner;
  }

  @Timed
  @GET
  @Path("/attachment/{attachmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public AttachmentUri getSignedAttachmentUri(@Auth                      FederatedPeer peer,
                                              @PathParam("attachmentId") long attachmentId)
  {
    URL url = urlSigner.getPreSignedUrl(attachmentId, HttpMethod.GET);
    return new AttachmentUri(url);
  }

  @Timed
  @GET
  @Path("/key/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public UnstructuredPreKeyList getKey(@Auth                FederatedPeer peer,
                       @PathParam("number") String number)
  {
    UnstructuredPreKeyList preKeys = keys.get(number, accounts.getAllByNumber(number));

    if (preKeys == null) {
      throw new WebApplicationException(Response.status(404).build());
    }

    return preKeys;
  }

  @Timed
  @PUT
  @Path("/message")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public MessageResponse relayMessage(@Auth FederatedPeer peer, @Valid List<RelayMessage> messages)
      throws IOException
  {
    try {
      Map<String, Pair<Boolean, Set<Long>>> destinations = new HashMap<>();

      for (RelayMessage message : messages) {
        Pair<Boolean, Set<Long>> deviceIds = destinations.get(message.getDestination());
        if (deviceIds == null) {
          deviceIds = new Pair<Boolean, Set<Long>>(true, new HashSet<Long>());
          destinations.put(message.getDestination(), deviceIds);
        }
        deviceIds.second().add(message.getDestinationDeviceId());
      }

      Map<Pair<String, Long>, Account> accountCache = new HashMap<>();
      List<String> numbersMissingDevices = new LinkedList<>();
      pushSender.fillLocalAccountsCache(destinations, accountCache, numbersMissingDevices);

      List<String> success = new LinkedList<>();
      List<String> failure = new LinkedList<>(numbersMissingDevices);

      for (RelayMessage message : messages) {
        Account account = accountCache.get(new Pair<>(message.getDestination(), message.getDestinationDeviceId()));
        if (account == null)
          continue;
        OutgoingMessageSignal signal = OutgoingMessageSignal.parseFrom(message.getOutgoingMessageSignal())
                                                            .toBuilder()
                                                            .setRelay(peer.getName())
                                                            .build();
        try {
          pushSender.sendMessage(account, signal);
        } catch (NoSuchUserException e) {
          logger.info("No such user", e);
          failure.add(message.getDestination());
        }
      }

      return new MessageResponse(success, failure, numbersMissingDevices);
    } catch (InvalidProtocolBufferException ipe) {
      logger.warn("ProtoBuf", ipe);
      throw new WebApplicationException(Response.status(400).build());
    }
  }

  @Timed
  @GET
  @Path("/user_count")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountCount getUserCount(@Auth FederatedPeer peer) {
    return new AccountCount((int)accounts.getCount());
  }

  @Timed
  @GET
  @Path("/user_tokens/{offset}")
  @Produces(MediaType.APPLICATION_JSON)
  public ClientContacts getUserTokens(@Auth                FederatedPeer peer,
                                      @PathParam("offset") int offset)
  {
    List<Account>        numberList    = accounts.getAllMasterAccounts(offset, ACCOUNT_CHUNK_SIZE);
    List<ClientContact> clientContacts = new LinkedList<>();

    for (Account account : numberList) {
      byte[]        token         = Util.getContactToken(account.getNumber());
      ClientContact clientContact = new ClientContact(token, null, account.getSupportsSms());

      if (!account.isActive())
        clientContact.setInactive(true);

      clientContacts.add(clientContact);
    }

    return new ClientContacts(clientContacts);
  }
}
