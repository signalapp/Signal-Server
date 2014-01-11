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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import com.yammer.dropwizard.auth.AuthenticationException;
import com.yammer.dropwizard.auth.basic.BasicCredentials;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DeviceAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;
import org.whispersystems.textsecuregcm.entities.MessageResponse;
import org.whispersystems.textsecuregcm.entities.RelayMessage;
import org.whispersystems.textsecuregcm.federation.FederatedClient;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.IterablePair;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MessageController extends HttpServlet {

  public static final String PATH = "/v1/messages/";

  private final Meter  successMeter = Metrics.newMeter(MessageController.class, "deliver_message", "success", TimeUnit.MINUTES);
  private final Meter  failureMeter = Metrics.newMeter(MessageController.class, "deliver_message", "failure", TimeUnit.MINUTES);
  private final Timer  timer        = Metrics.newTimer(MessageController.class, "deliver_message_time", TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
  private final Logger logger       = LoggerFactory.getLogger(MessageController.class);

  private final RateLimiters           rateLimiters;
  private final DeviceAuthenticator    deviceAuthenticator;
  private final PushSender             pushSender;
  private final FederatedClientManager federatedClientManager;
  private final ObjectMapper           objectMapper;
  private final ExecutorService        executor;
  private final AccountsManager        accountsManager;

  public MessageController(RateLimiters rateLimiters,
                           DeviceAuthenticator deviceAuthenticator,
                           PushSender pushSender,
                           AccountsManager accountsManager,
                           FederatedClientManager federatedClientManager)
  {
    this.rateLimiters           = rateLimiters;
    this.deviceAuthenticator    = deviceAuthenticator;
    this.pushSender             = pushSender;
    this.accountsManager        = accountsManager;
    this.federatedClientManager = federatedClientManager;
    this.objectMapper           = new ObjectMapper();
    this.executor               = Executors.newFixedThreadPool(10);
  }

  class LocalOrRemoteDevice {
    Device device;
    String relay, number; long deviceId;
    LocalOrRemoteDevice(Device device) {
      this.device = device; this.number = device.getNumber(); this.deviceId = device.getDeviceId();
    }
    LocalOrRemoteDevice(String relay, String number, long deviceId) {
      this.relay = relay; this.number = number; this.deviceId = deviceId;
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
    TimerContext timerContext = timer.time();

    try {
      Device sender   = authenticate(req);
      IncomingMessageList messages = parseIncomingMessages(req);

      rateLimiters.getMessagesLimiter().validate(sender.getNumber());

      List<String> numbersMissingDevices = new LinkedList<>();

      List<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>> outgoingMessages =
          getOutgoingMessageSignals(sender.getNumber(), messages.getMessages(), numbersMissingDevices);

      handleAsyncDelivery(timerContext, req.startAsync(), outgoingMessages, numbersMissingDevices);
    } catch (AuthenticationException e) {
      failureMeter.mark();
      timerContext.stop();
      resp.setStatus(401);
    } catch (ValidationException e) {
      failureMeter.mark();
      timerContext.stop();
      resp.setStatus(415);
    } catch (IOException e) {
      logger.warn("IOE", e);
      failureMeter.mark();
      timerContext.stop();
      resp.setStatus(501);
    } catch (RateLimitExceededException e) {
      timerContext.stop();
      failureMeter.mark();
      resp.setStatus(413);
    }
  }

  private void handleAsyncDelivery(final TimerContext timerContext,
                                   final AsyncContext context,
                                   final List<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>> listPair,
                                   final List<String> numbersMissingDevices)
  {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        List<String>        success  = new LinkedList<>();
        List<String>        failure  = new LinkedList<>(numbersMissingDevices);
        HttpServletResponse response = (HttpServletResponse) context.getResponse();

        try {
          Map<String, Set<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>>> relayMessages = new HashMap<>();
          for (Pair<LocalOrRemoteDevice, OutgoingMessageSignal> messagePair : listPair) {
            String relay = messagePair.first().relay;

            if (Util.isEmpty(relay)) {
              String encodedId = messagePair.first().device.getBackwardsCompatibleNumberEncoding();
              try {
                pushSender.sendMessage(messagePair.first().device, messagePair.second());
                success.add(encodedId);
              } catch (NoSuchUserException e) {
                logger.debug("No such user", e);
                failure.add(encodedId);
              }
            } else {
              Set<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>> messageSet = relayMessages.get(relay);
              if (messageSet == null) {
                messageSet = new HashSet<>();
                relayMessages.put(relay, messageSet);
              }
              messageSet.add(messagePair);
            }
          }

          for (Map.Entry<String, Set<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>>> messagesForRelay : relayMessages.entrySet()) {
            try {
              FederatedClient client = federatedClientManager.getClient(messagesForRelay.getKey());

              List<RelayMessage> messages = new LinkedList<>();
              for (Pair<LocalOrRemoteDevice, OutgoingMessageSignal> message : messagesForRelay.getValue()) {
                messages.add(new RelayMessage(message.first().number,
                                              message.first().deviceId,
                                              message.second().toByteArray()));
              }

              MessageResponse relayResponse = client.sendMessages(messages);
              for (String string : relayResponse.getSuccess())
                success.add(string);
              for (String string : relayResponse.getFailure())
                failure.add(string);
              for (String string : relayResponse.getNumbersMissingDevices())
                numbersMissingDevices.add(string);
            } catch (NoSuchPeerException e) {
              logger.info("No such peer", e);
              for (Pair<LocalOrRemoteDevice, OutgoingMessageSignal> messagePair : messagesForRelay.getValue())
                failure.add(messagePair.first().number);
            }
          }

          byte[] responseData = serializeResponse(new MessageResponse(success, failure, numbersMissingDevices));
          response.setContentLength(responseData.length);
          response.getOutputStream().write(responseData);
          context.complete();
          successMeter.mark();
        } catch (IOException e) {
          logger.warn("Async Handler", e);
          failureMeter.mark();
          response.setStatus(501);
          context.complete();
        } catch (Exception e) {
          logger.error("Unknown error sending message", e);
          failureMeter.mark();
          response.setStatus(500);
          context.complete();
        }

        timerContext.stop();
      }
    });
  }

  @Nullable
  private List<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>> getOutgoingMessageSignals(String sourceNumber,
                                                                                           List<IncomingMessage> incomingMessages,
                                                                                           List<String> numbersMissingDevices)
  {
    List<Pair<LocalOrRemoteDevice, OutgoingMessageSignal>> outgoingMessages = new LinkedList<>();
    Map<String, Set<Long>> localDestinations = new HashMap<>();
    Set<String> destinationNumbers = new HashSet<>();
    for (IncomingMessage incoming : incomingMessages) {
      destinationNumbers.add(incoming.getDestination());
      if (!Util.isEmpty(incoming.getRelay()))
        continue;

      Set<Long> deviceIds = localDestinations.get(incoming.getDestination());
      if (deviceIds == null) {
        deviceIds = new HashSet<>();
        localDestinations.put(incoming.getDestination(), deviceIds);
      }
      deviceIds.add(incoming.getDestinationDeviceId());
    }

    Pair<Map<String, Account>, List<String>> accountsForDevices = accountsManager.getAccountsForDevices(localDestinations);

    Map<String, Account> localAccounts         = accountsForDevices.first();
    for (String number : accountsForDevices.second())
      numbersMissingDevices.add(number);

    for (IncomingMessage incoming : incomingMessages) {
      OutgoingMessageSignal.Builder outgoingMessage = OutgoingMessageSignal.newBuilder();
      outgoingMessage.setType(incoming.getType());
      outgoingMessage.setSource(sourceNumber);

      byte[] messageBody = getMessageBody(incoming);

      if (messageBody != null) {
        outgoingMessage.setMessage(ByteString.copyFrom(messageBody));
      }

      outgoingMessage.setTimestamp(System.currentTimeMillis());

      int index = 0;

      for (String destination : destinationNumbers) {
        if (!destination.equals(incoming.getDestination()))
          outgoingMessage.setDestinations(index++, destination);
      }

      LocalOrRemoteDevice device = null;
      if (!Util.isEmpty(incoming.getRelay()))
        device = new LocalOrRemoteDevice(incoming.getRelay(), incoming.getDestination(), incoming.getDestinationDeviceId());
      else {
        Account destination = localAccounts.get(incoming.getDestination());
        if (destination != null)
          device = new LocalOrRemoteDevice(destination.getDevice(incoming.getDestinationDeviceId()));
      }

      if (device != null)
        outgoingMessages.add(new Pair<>(device, outgoingMessage.build()));
    }

    return outgoingMessages;
  }

  private byte[] getMessageBody(IncomingMessage message) {
    try {
      return Base64.decode(message.getBody());
    } catch (IOException ioe) {
      ioe.printStackTrace();
      return null;
    }
  }

  private byte[] serializeResponse(MessageResponse response) throws IOException {
    try {
      return objectMapper.writeValueAsBytes(response);
    } catch (JsonProcessingException e) {
      throw new IOException(e);
    }
  }

  private IncomingMessageList parseIncomingMessages(HttpServletRequest request)
      throws IOException, ValidationException
  {
    BufferedReader reader  = request.getReader();
    StringBuilder  content = new StringBuilder();
    String         line;

    while ((line = reader.readLine()) != null) {
      content.append(line);
    }

    IncomingMessageList messages = objectMapper.readValue(content.toString(),
                                                          IncomingMessageList.class);

    if (messages.getMessages() == null) {
      throw new ValidationException();
    }

    for (IncomingMessage message : messages.getMessages()) {
      if (message.getBody() == null)        throw new ValidationException();
      if (message.getDestination() == null) throw new ValidationException();
    }

    return messages;
  }

  private Device authenticate(HttpServletRequest request) throws AuthenticationException {
    try {
      AuthorizationHeader authorizationHeader = AuthorizationHeader.fromFullHeader(request.getHeader("Authorization"));
      BasicCredentials    credentials         = new BasicCredentials(authorizationHeader.getNumber() + "." + authorizationHeader.getDeviceId(),
                                                                     authorizationHeader.getPassword()  );

      Optional<Device> account = deviceAuthenticator.authenticate(credentials);

      if (account.isPresent()) return account.get();
      else                     throw new AuthenticationException("Bad credentials");
    } catch (InvalidAuthorizationHeaderException e) {
      throw new AuthenticationException(e);
    }
  }




//  @Timed
//  @POST
//  @Consumes(MediaType.APPLICATION_JSON)
//  @Produces(MediaType.APPLICATION_JSON)
//  public MessageResponse sendMessage(@Auth Device sender, IncomingMessageList messages)
//      throws IOException
//  {
//    List<String>                success          = new LinkedList<>();
//    List<String>                failure          = new LinkedList<>();
//    List<IncomingMessage>       incomingMessages = messages.getMessages();
//    List<OutgoingMessageSignal> outgoingMessages = getOutgoingMessageSignals(sender.getNumber(), incomingMessages);
//
//    IterablePair<IncomingMessage, OutgoingMessageSignal> listPair = new IterablePair<>(incomingMessages, outgoingMessages);
//
//    for (Pair<IncomingMessage, OutgoingMessageSignal> messagePair : listPair) {
//      String destination = messagePair.first().getDestination();
//      String relay       = messagePair.first().getRelay();
//
//      try {
//        if (Util.isEmpty(relay)) sendLocalMessage(destination, messagePair.second());
//        else                     sendRelayMessage(relay, destination, messagePair.second());
//        success.add(destination);
//      } catch (NoSuchUserException e) {
//        logger.debug("No such user", e);
//        failure.add(destination);
//      }
//    }
//
//    return new MessageResponse(success, failure);
//  }

}
