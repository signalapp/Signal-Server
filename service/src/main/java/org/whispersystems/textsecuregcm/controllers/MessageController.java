/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/messages")
public class MessageController {

  private final Logger         logger                           = LoggerFactory.getLogger(MessageController.class);
  private final MetricRegistry metricRegistry                   = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          unidentifiedMeter                = metricRegistry.meter(name(getClass(), "delivery", "unidentified"));
  private final Meter          identifiedMeter                  = metricRegistry.meter(name(getClass(), "delivery", "identified"  ));
  private final Meter          rejectOversizeMessageMeter       = metricRegistry.meter(name(getClass(), "rejectOversizeMessage"));
  private final Timer          sendMessageInternalTimer         = metricRegistry.timer(name(getClass(), "sendMessageInternal"));
  private final Histogram      outgoingMessageListSizeHistogram = metricRegistry.histogram(name(getClass(), "outgoingMessageListSize"));

  private final RateLimiters           rateLimiters;
  private final MessageSender          messageSender;
  private final ReceiptSender          receiptSender;
  private final AccountsManager        accountsManager;
  private final MessagesManager        messagesManager;
  private final ApnFallbackManager     apnFallbackManager;

  private static final String CONTENT_SIZE_DISTRIBUTION_NAME                     = name(MessageController.class, "messageContentSize");
  private static final String OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME = name(MessageController.class, "outgoingMessageListSizeBytes");

  private static final int MAX_MESSAGE_SIZE = 1024 * 1024;

  public MessageController(RateLimiters rateLimiters,
                           MessageSender messageSender,
                           ReceiptSender receiptSender,
                           AccountsManager accountsManager,
                           MessagesManager messagesManager,
                           ApnFallbackManager apnFallbackManager)
  {
    this.rateLimiters           = rateLimiters;
    this.messageSender          = messageSender;
    this.receiptSender          = receiptSender;
    this.accountsManager        = accountsManager;
    this.messagesManager        = messagesManager;
    this.apnFallbackManager     = apnFallbackManager;
  }

  @Timed
  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public SendMessageResponse sendMessage(@Auth                                     Optional<Account>   source,
                                         @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
                                         @HeaderParam("User-Agent")                String userAgent,
                                         @PathParam("destination")                 AmbiguousIdentifier destinationName,
                                         @Valid                                    IncomingMessageList messages)
      throws RateLimitExceededException
  {
    if (!source.isPresent() && !accessKey.isPresent()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    if (source.isPresent() && !source.get().isFor(destinationName)) {
      rateLimiters.getMessagesLimiter().validate(source.get().getNumber() + "__" + destinationName);
    }

    if (source.isPresent() && !source.get().isFor(destinationName)) {
      identifiedMeter.mark();
    } else if (!source.isPresent()) {
      unidentifiedMeter.mark();
    }

    for (final IncomingMessage message : messages.getMessages()) {
      int contentLength = 0;

      if (!Util.isEmpty(message.getContent())) {
        contentLength += message.getContent().length();
      }

      if (!Util.isEmpty(message.getBody())) {
        contentLength += message.getBody().length();
      }

      Metrics.summary(CONTENT_SIZE_DISTRIBUTION_NAME, UserAgentTagUtil.getUserAgentTags(userAgent)).record(contentLength);

      if (contentLength > MAX_MESSAGE_SIZE) {
        // TODO Reject the request
        rejectOversizeMessageMeter.mark();
      }
    }

    try {
      boolean isSyncMessage = source.isPresent() && source.get().isFor(destinationName);

      Optional<Account> destination;

      if (!isSyncMessage) destination = accountsManager.get(destinationName);
      else                destination = source;

      OptionalAccess.verify(source, accessKey, destination);
      assert(destination.isPresent());

      validateCompleteDeviceList(destination.get(), messages.getMessages(), isSyncMessage);
      validateRegistrationIds(destination.get(), messages.getMessages());

      for (IncomingMessage incomingMessage : messages.getMessages()) {
        Optional<Device> destinationDevice = destination.get().getDevice(incomingMessage.getDestinationDeviceId());

        if (destinationDevice.isPresent()) {
          sendMessage(source, destination.get(), destinationDevice.get(), messages.getTimestamp(), messages.isOnline(), incomingMessage);
        }
      }

      return new SendMessageResponse(!isSyncMessage && source.isPresent() && source.get().getEnabledDeviceCount() > 1);
    } catch (NoSuchUserException e) {
      throw new WebApplicationException(Response.status(404).build());
    } catch (MismatchedDevicesException e) {
      throw new WebApplicationException(Response.status(409)
                                                .type(MediaType.APPLICATION_JSON_TYPE)
                                                .entity(new MismatchedDevices(e.getMissingDevices(),
                                                                              e.getExtraDevices()))
                                                .build());
    } catch (StaleDevicesException e) {
      throw new WebApplicationException(Response.status(410)
                                                .type(MediaType.APPLICATION_JSON)
                                                .entity(new StaleDevices(e.getStaleDevices()))
                                                .build());
    }
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public OutgoingMessageEntityList getPendingMessages(@Auth Account account, @HeaderParam("User-Agent") String userAgent) {
    assert account.getAuthenticatedDevice().isPresent();

    if (!Util.isEmpty(account.getAuthenticatedDevice().get().getApnId())) {
      RedisOperation.unchecked(() -> apnFallbackManager.cancel(account, account.getAuthenticatedDevice().get()));
    }

    final OutgoingMessageEntityList outgoingMessages = messagesManager.getMessagesForDevice(account.getNumber(),
                                                                                            account.getUuid(),
                                                                                            account.getAuthenticatedDevice().get().getId(),
                                                                                            userAgent,
                                                                                            false);

    outgoingMessageListSizeHistogram.update(outgoingMessages.getMessages().size());

    {
      String platform;

      try {
        platform = UserAgentUtil.parseUserAgentString(userAgent).getPlatform().name().toLowerCase();
      } catch (final UnrecognizedUserAgentException ignored) {
        platform = "unrecognized";
      }

      Metrics.summary(OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME, "platform", platform).record(estimateMessageListSizeBytes(outgoingMessages));
    }

    return outgoingMessages;
  }

  private static long estimateMessageListSizeBytes(final OutgoingMessageEntityList messageList) {
    long size = 0;

    for (final OutgoingMessageEntity message : messageList.getMessages()) {
      size += message.getContent() == null      ? 0 : message.getContent().length;
      size += message.getMessage() == null      ? 0 : message.getMessage().length;
      size += Util.isEmpty(message.getSource()) ? 0 : message.getSource().length();
      size += Util.isEmpty(message.getRelay())  ? 0 : message.getRelay().length();
    }

    return size;
  }

  @Timed
  @DELETE
  @Path("/{source}/{timestamp}")
  public void removePendingMessage(@Auth Account account,
                                   @PathParam("source") String source,
                                   @PathParam("timestamp") long timestamp)
  {
    try {
      WebSocketConnection.recordMessageDeliveryDuration(timestamp, account.getAuthenticatedDevice().get());
      Optional<OutgoingMessageEntity> message = messagesManager.delete(account.getNumber(),
                                                                       account.getUuid(),
                                                                       account.getAuthenticatedDevice().get().getId(),
                                                                       source, timestamp);

      if (message.isPresent() && message.get().getType() != Envelope.Type.RECEIPT_VALUE) {
        receiptSender.sendReceipt(account,
                                  message.get().getSource(),
                                  message.get().getTimestamp());
      }
    } catch (NoSuchUserException e) {
      logger.warn("Sending delivery receipt", e);
    }
  }

  @Timed
  @DELETE
  @Path("/uuid/{uuid}")
  public void removePendingMessage(@Auth Account account, @PathParam("uuid") UUID uuid) {
    try {
      Optional<OutgoingMessageEntity> message = messagesManager.delete(account.getNumber(),
                                                                       account.getUuid(),
                                                                       account.getAuthenticatedDevice().get().getId(),
                                                                       uuid);

      if (message.isPresent()) {
        WebSocketConnection.recordMessageDeliveryDuration(message.get().getTimestamp(), account.getAuthenticatedDevice().get());
        if (!Util.isEmpty(message.get().getSource()) && message.get().getType() != Envelope.Type.RECEIPT_VALUE) {
          receiptSender.sendReceipt(account, message.get().getSource(), message.get().getTimestamp());
        }
      }

    } catch (NoSuchUserException e) {
      logger.warn("Sending delivery receipt", e);
    }
  }

  private void sendMessage(Optional<Account> source,
                           Account destinationAccount,
                           Device destinationDevice,
                           long timestamp,
                           boolean online,
                           IncomingMessage incomingMessage)
      throws NoSuchUserException
  {
    try (final Timer.Context ignored = sendMessageInternalTimer.time()) {
      Optional<byte[]> messageBody    = getMessageBody(incomingMessage);
      Optional<byte[]> messageContent = getMessageContent(incomingMessage);
      Envelope.Builder messageBuilder = Envelope.newBuilder();

      messageBuilder.setType(Envelope.Type.valueOf(incomingMessage.getType()))
                    .setTimestamp(timestamp == 0 ? System.currentTimeMillis() : timestamp)
                    .setServerTimestamp(System.currentTimeMillis());

      if (source.isPresent()) {
        messageBuilder.setSource(source.get().getNumber())
                      .setSourceUuid(source.get().getUuid().toString())
                      .setSourceDevice((int)source.get().getAuthenticatedDevice().get().getId());
      }

      if (messageBody.isPresent()) {
        messageBuilder.setLegacyMessage(ByteString.copyFrom(messageBody.get()));
      }

      if (messageContent.isPresent()) {
        messageBuilder.setContent(ByteString.copyFrom(messageContent.get()));
      }

      messageSender.sendMessage(destinationAccount, destinationDevice, messageBuilder.build(), online);
    } catch (NotPushRegisteredException e) {
      if (destinationDevice.isMaster()) throw new NoSuchUserException(e);
      else                              logger.debug("Not registered", e);
    }
  }

  private void validateRegistrationIds(Account account, List<IncomingMessage> messages)
      throws StaleDevicesException
  {
    List<Long> staleDevices = new LinkedList<>();

    for (IncomingMessage message : messages) {
      Optional<Device> device = account.getDevice(message.getDestinationDeviceId());

      if (device.isPresent() &&
          message.getDestinationRegistrationId() > 0 &&
          message.getDestinationRegistrationId() != device.get().getRegistrationId())
      {
        staleDevices.add(device.get().getId());
      }
    }

    if (!staleDevices.isEmpty()) {
      throw new StaleDevicesException(staleDevices);
    }
  }

  private void validateCompleteDeviceList(Account account,
                                          List<IncomingMessage> messages,
                                          boolean isSyncMessage)
      throws MismatchedDevicesException
  {
    Set<Long> messageDeviceIds = new HashSet<>();
    Set<Long> accountDeviceIds = new HashSet<>();

    List<Long> missingDeviceIds = new LinkedList<>();
    List<Long> extraDeviceIds   = new LinkedList<>();

    for (IncomingMessage message : messages) {
      messageDeviceIds.add(message.getDestinationDeviceId());
    }

    for (Device device : account.getDevices()) {
      if (device.isEnabled() &&
          !(isSyncMessage && device.getId() == account.getAuthenticatedDevice().get().getId()))
      {
        accountDeviceIds.add(device.getId());

        if (!messageDeviceIds.contains(device.getId())) {
          missingDeviceIds.add(device.getId());
        }
      }
    }

    for (IncomingMessage message : messages) {
      if (!accountDeviceIds.contains(message.getDestinationDeviceId())) {
        extraDeviceIds.add(message.getDestinationDeviceId());
      }
    }

    if (!missingDeviceIds.isEmpty() || !extraDeviceIds.isEmpty()) {
      throw new MismatchedDevicesException(missingDeviceIds, extraDeviceIds);
    }
  }

  private Optional<byte[]> getMessageBody(IncomingMessage message) {
    if (Util.isEmpty(message.getBody())) return Optional.empty();

    try {
      return Optional.of(Base64.decode(message.getBody()));
    } catch (IOException ioe) {
      logger.debug("Bad B64", ioe);
      return Optional.empty();
    }
  }

  private Optional<byte[]> getMessageContent(IncomingMessage message) {
    if (Util.isEmpty(message.getContent())) return Optional.empty();

    try {
      return Optional.of(Base64.decode(message.getContent()));
    } catch (IOException ioe) {
      logger.debug("Bad B64", ioe);
      return Optional.empty();
    }
  }
}
