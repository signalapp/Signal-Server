/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.dropwizard.auth.Auth;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.abuse.FilterAbusiveMessages;
import org.whispersystems.textsecuregcm.auth.Anonymous;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CombinedUnidentifiedSenderAccessKeys;
import org.whispersystems.textsecuregcm.auth.OptionalAccess;
import org.whispersystems.textsecuregcm.entities.AccountMismatchedDevices;
import org.whispersystems.textsecuregcm.entities.AccountStaleDevices;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope.Type;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.MultiRecipientMessage;
import org.whispersystems.textsecuregcm.entities.MultiRecipientMessage.Recipient;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntityList;
import org.whispersystems.textsecuregcm.entities.SendMessageResponse;
import org.whispersystems.textsecuregcm.entities.SendMultiRecipientMessageResponse;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeException;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.limits.UnsealedSenderRateLimiter;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ReportMessageManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/messages")
public class MessageController {

  private final Logger         logger                           = LoggerFactory.getLogger(MessageController.class);
  private final MetricRegistry metricRegistry                   = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          unidentifiedMeter                = metricRegistry.meter(name(getClass(), "delivery", "unidentified"));
  private final Meter          identifiedMeter                  = metricRegistry.meter(name(getClass(), "delivery", "identified"  ));
  private final Meter          rejectOver256kibMessageMeter     = metricRegistry.meter(name(getClass(), "rejectOver256kibMessage"));
  private final Timer          sendMessageInternalTimer         = metricRegistry.timer(name(getClass(), "sendMessageInternal"));
  private final Timer          sendCommonMessageInternalTimer   = metricRegistry.timer(name(getClass(), "sendCommonMessageInternal"));
  private final Histogram      outgoingMessageListSizeHistogram = metricRegistry.histogram(name(getClass(), "outgoingMessageListSize"));

  private final RateLimiters                rateLimiters;
  private final MessageSender               messageSender;
  private final ReceiptSender               receiptSender;
  private final AccountsManager             accountsManager;
  private final MessagesManager             messagesManager;
  private final UnsealedSenderRateLimiter   unsealedSenderRateLimiter;
  private final ApnFallbackManager          apnFallbackManager;
  private final RateLimitChallengeManager   rateLimitChallengeManager;
  private final ReportMessageManager        reportMessageManager;
  private final ExecutorService             multiRecipientMessageExecutor;

  private static final String LEGACY_MESSAGE_SENT_COUNTER = name(MessageController.class, "legacyMessageSent");
  private static final String SENT_MESSAGE_COUNTER_NAME                          = name(MessageController.class, "sentMessages");
  private static final String REJECT_UNSEALED_SENDER_COUNTER_NAME                = name(MessageController.class, "rejectUnsealedSenderLimit");
  private static final String CONTENT_SIZE_DISTRIBUTION_NAME                     = name(MessageController.class, "messageContentSize");
  private static final String OUTGOING_MESSAGE_LIST_SIZE_BYTES_DISTRIBUTION_NAME = name(MessageController.class, "outgoingMessageListSizeBytes");

  private static final String EPHEMERAL_TAG_NAME      = "ephemeral";
  private static final String SENDER_TYPE_TAG_NAME    = "senderType";
  private static final String SENDER_COUNTRY_TAG_NAME = "senderCountry";

  private static final long MAX_MESSAGE_SIZE = DataSize.kibibytes(256).toBytes();

  public MessageController(
      RateLimiters rateLimiters,
      MessageSender messageSender,
      ReceiptSender receiptSender,
      AccountsManager accountsManager,
      MessagesManager messagesManager,
      UnsealedSenderRateLimiter unsealedSenderRateLimiter,
      ApnFallbackManager apnFallbackManager,
      RateLimitChallengeManager rateLimitChallengeManager,
      ReportMessageManager reportMessageManager,
      @Nonnull ExecutorService multiRecipientMessageExecutor) {
    this.rateLimiters = rateLimiters;
    this.messageSender = messageSender;
    this.receiptSender = receiptSender;
    this.accountsManager = accountsManager;
    this.messagesManager = messagesManager;
    this.unsealedSenderRateLimiter = unsealedSenderRateLimiter;
    this.apnFallbackManager = apnFallbackManager;
    this.rateLimitChallengeManager = rateLimitChallengeManager;
    this.reportMessageManager = reportMessageManager;
    this.multiRecipientMessageExecutor = Objects.requireNonNull(multiRecipientMessageExecutor);
  }

  @Timed
  @Path("/{destination}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @FilterAbusiveMessages
  public Response sendMessage(@Auth Optional<AuthenticatedAccount> source,
      @HeaderParam(OptionalAccess.UNIDENTIFIED) Optional<Anonymous> accessKey,
      @HeaderParam("User-Agent") String userAgent,
      @HeaderParam("X-Forwarded-For") String forwardedFor,
      @PathParam("destination") UUID destinationUuid,
      @Valid IncomingMessageList messages)
      throws RateLimitExceededException, RateLimitChallengeException {

    if (source.isEmpty() && accessKey.isEmpty()) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    final String senderType;

    if (source.isPresent() && !source.get().getAccount().getUuid().equals(destinationUuid)) {
      identifiedMeter.mark();
      senderType = "identified";
    } else if (source.isEmpty()) {
      unidentifiedMeter.mark();
      senderType = "unidentified";
    } else {
      senderType = "self";
    }

    for (final IncomingMessage message : messages.getMessages()) {
      int contentLength = 0;

      if (!Util.isEmpty(message.getContent())) {
        contentLength += message.getContent().length();
      }

      if (!Util.isEmpty(message.getBody())) {
        contentLength += message.getBody().length();
      }

      Metrics.summary(CONTENT_SIZE_DISTRIBUTION_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).record(contentLength);

      if (contentLength > MAX_MESSAGE_SIZE) {
        rejectOver256kibMessageMeter.mark();
        return Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE).build();
      }
    }

    try {
      boolean isSyncMessage = source.isPresent() && source.get().getAccount().getUuid().equals(destinationUuid);

      Optional<Account> destination;

      if (!isSyncMessage) {
        destination = accountsManager.get(destinationUuid);
      } else {
        destination = source.map(AuthenticatedAccount::getAccount);
      }

      OptionalAccess.verify(source.map(AuthenticatedAccount::getAccount), accessKey, destination);
      assert (destination.isPresent());

      if (source.isPresent() && !source.get().getAccount().getUuid().equals(destinationUuid)) {
        rateLimiters.getMessagesLimiter().validate(source.get().getAccount().getUuid(), destination.get().getUuid());

        final String senderCountryCode = Util.getCountryCode(source.get().getAccount().getNumber());

        try {
          unsealedSenderRateLimiter.validate(source.get().getAccount(), destination.get());
        } catch (final RateLimitExceededException e) {

          final boolean legacyClient = rateLimitChallengeManager.isClientBelowMinimumVersion(userAgent);

          Metrics.counter(REJECT_UNSEALED_SENDER_COUNTER_NAME,
                  SENDER_COUNTRY_TAG_NAME, senderCountryCode,
                  "legacyClient", String.valueOf(legacyClient))
              .increment();

          if (legacyClient) {
            throw e;
          }

          throw new RateLimitChallengeException(source.get().getAccount(), e.getRetryDuration());
        }
      }

      validateCompleteDeviceList(destination.get(), messages.getMessages(), isSyncMessage,
          source.map(AuthenticatedAccount::getAuthenticatedDevice).map(Device::getId));
      validateRegistrationIds(destination.get(), messages.getMessages());

      final List<Tag> tags = List.of(UserAgentTagUtil.getPlatformTag(userAgent),
          Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(messages.isOnline())),
          Tag.of(SENDER_TYPE_TAG_NAME, senderType));

      for (IncomingMessage incomingMessage : messages.getMessages()) {
        Optional<Device> destinationDevice = destination.get().getDevice(incomingMessage.getDestinationDeviceId());

        if (destinationDevice.isPresent()) {
          Metrics.counter(SENT_MESSAGE_COUNTER_NAME, tags).increment();
          sendMessage(source, destination.get(), destinationDevice.get(), messages.getTimestamp(), messages.isOnline(),
              incomingMessage);
        }
      }

      return Response.ok(new SendMessageResponse(
          !isSyncMessage && source.isPresent() && source.get().getAccount().getEnabledDeviceCount() > 1)).build();
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
  @Path("/multi_recipient")
  @PUT
  @Consumes(MultiRecipientMessageProvider.MEDIA_TYPE)
  @Produces(MediaType.APPLICATION_JSON)
  @FilterAbusiveMessages
  public Response sendMultiRecipientMessage(
      @HeaderParam(OptionalAccess.UNIDENTIFIED) CombinedUnidentifiedSenderAccessKeys accessKeys,
      @HeaderParam("User-Agent") String userAgent,
      @HeaderParam("X-Forwarded-For") String forwardedFor,
      @QueryParam("online") boolean online,
      @QueryParam("ts") long timestamp,
      @Valid MultiRecipientMessage multiRecipientMessage) {

    unidentifiedMeter.mark(multiRecipientMessage.getRecipients().length);

    Map<UUID, Account> uuidToAccountMap = Arrays.stream(multiRecipientMessage.getRecipients())
        .map(Recipient::getUuid)
        .distinct()
        .collect(Collectors.toUnmodifiableMap(Function.identity(), uuid -> {
          Optional<Account> account = accountsManager.get(uuid);
          if (account.isEmpty()) {
            throw new WebApplicationException(Status.NOT_FOUND);
          }
          return account.get();
        }));
    checkAccessKeys(accessKeys, uuidToAccountMap);

    final Map<Account, HashSet<Pair<Long, Integer>>> accountToDeviceIdAndRegistrationIdMap =
        Arrays
            .stream(multiRecipientMessage.getRecipients())
            .collect(Collectors.toMap(
                recipient -> uuidToAccountMap.get(recipient.getUuid()),
                recipient -> new HashSet<>(
                    Collections.singletonList(new Pair<>(recipient.getDeviceId(), recipient.getRegistrationId()))),
                (a, b) -> {
                  a.addAll(b);
                  return a;
                }
            ));

    Collection<AccountMismatchedDevices> accountMismatchedDevices = new ArrayList<>();
    Collection<AccountStaleDevices> accountStaleDevices = new ArrayList<>();
    uuidToAccountMap.values().forEach(account -> {
      final Set<Pair<Long, Integer>> deviceIdAndRegistrationIdSet = accountToDeviceIdAndRegistrationIdMap.get(account);
      final Set<Long> deviceIds = deviceIdAndRegistrationIdSet.stream().map(Pair::first).collect(Collectors.toSet());
      try {
        validateCompleteDeviceList(account, deviceIds, false, Optional.empty());
        validateRegistrationIds(account, deviceIdAndRegistrationIdSet.stream());
      } catch (MismatchedDevicesException e) {
        accountMismatchedDevices.add(new AccountMismatchedDevices(account.getUuid(),
            new MismatchedDevices(e.getMissingDevices(), e.getExtraDevices())));
      } catch (StaleDevicesException e) {
        accountStaleDevices.add(new AccountStaleDevices(account.getUuid(), new StaleDevices(e.getStaleDevices())));
      }
    });
    if (!accountMismatchedDevices.isEmpty()) {
      return Response
          .status(409)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(accountMismatchedDevices)
          .build();
    }
    if (!accountStaleDevices.isEmpty()) {
      return Response
          .status(410)
          .type(MediaType.APPLICATION_JSON)
          .entity(accountStaleDevices)
          .build();
    }

    List<Tag> tags = List.of(
        UserAgentTagUtil.getPlatformTag(userAgent),
        Tag.of(EPHEMERAL_TAG_NAME, String.valueOf(online)),
        Tag.of(SENDER_TYPE_TAG_NAME, "unidentified"));
    List<UUID> uuids404 = Collections.synchronizedList(new ArrayList<>());
    final Counter counter = Metrics.counter(SENT_MESSAGE_COUNTER_NAME, tags);
    try {
      multiRecipientMessageExecutor.invokeAll(Arrays.stream(multiRecipientMessage.getRecipients())
          .map(recipient -> (Callable<Void>) () -> {
            Account destinationAccount = uuidToAccountMap.get(recipient.getUuid());

            // we asserted this must exist in validateCompleteDeviceList
            Device destinationDevice = destinationAccount.getDevice(recipient.getDeviceId()).orElseThrow();
            counter.increment();
            try {
              sendMessage(destinationAccount, destinationDevice, timestamp, online, recipient,
                  multiRecipientMessage.getCommonPayload());
            } catch (NoSuchUserException e) {
              uuids404.add(destinationAccount.getUuid());
            }
            return null;
          })
          .collect(Collectors.toList()));
    } catch (InterruptedException e) {
      logger.error("interrupted while delivering multi-recipient messages", e);
      return Response.serverError().entity("interrupted during delivery").build();
    }
    return Response.ok(new SendMultiRecipientMessageResponse(uuids404)).build();
  }

  private void checkAccessKeys(CombinedUnidentifiedSenderAccessKeys accessKeys, Map<UUID, Account> uuidToAccountMap) {
    AtomicBoolean throwUnauthorized = new AtomicBoolean(false);
    byte[] empty = new byte[16];
    final Optional<byte[]> UNRESTRICTED_UNIDENTIFIED_ACCESS_KEY = Optional.of(new byte[16]);
    byte[] combinedUnknownAccessKeys = uuidToAccountMap.values().stream()
        .map(account -> {
          if (account.isUnrestrictedUnidentifiedAccess()) {
            return UNRESTRICTED_UNIDENTIFIED_ACCESS_KEY;
          } else {
            return account.getUnidentifiedAccessKey();
          }
        })
        .map(accessKey -> {
          if (accessKey.isEmpty()) {
            throwUnauthorized.set(true);
            return empty;
          }
          return accessKey.get();
        })
        .reduce(new byte[16], (bytes, bytes2) -> {
          if (bytes.length != bytes2.length) {
            throwUnauthorized.set(true);
            return bytes;
          }
          for (int i = 0; i < bytes.length; i++) {
            bytes[i] ^= bytes2[i];
          }
          return bytes;
        });
    if (throwUnauthorized.get()
        || !MessageDigest.isEqual(combinedUnknownAccessKeys, accessKeys.getAccessKeys())) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public OutgoingMessageEntityList getPendingMessages(@Auth AuthenticatedAccount auth,
      @HeaderParam("User-Agent") String userAgent) {
    assert auth.getAuthenticatedDevice() != null;

    if (!Util.isEmpty(auth.getAuthenticatedDevice().getApnId())) {
      RedisOperation.unchecked(() -> apnFallbackManager.cancel(auth.getAccount(), auth.getAuthenticatedDevice()));
    }

    final OutgoingMessageEntityList outgoingMessages = messagesManager.getMessagesForDevice(
        auth.getAccount().getUuid(),
        auth.getAuthenticatedDevice().getId(),
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
  @Path("/uuid/{uuid}")
  public void removePendingMessage(@Auth AuthenticatedAccount auth, @PathParam("uuid") UUID uuid) {
    try {
      Optional<OutgoingMessageEntity> message = messagesManager.delete(
          auth.getAccount().getUuid(),
          auth.getAuthenticatedDevice().getId(),
          uuid);

      if (message.isPresent()) {
        WebSocketConnection.recordMessageDeliveryDuration(message.get().getTimestamp(), auth.getAuthenticatedDevice());
        if (!Util.isEmpty(message.get().getSource())
            && message.get().getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE) {
          receiptSender.sendReceipt(auth, message.get().getSourceUuid(), message.get().getTimestamp());
        }
      }

    } catch (NoSuchUserException e) {
      logger.warn("Sending delivery receipt", e);
    }
  }

  @Timed
  @POST
  @Path("/report/{sourceNumber}/{messageGuid}")
  public Response reportMessage(@Auth AuthenticatedAccount auth, @PathParam("sourceNumber") String sourceNumber,
      @PathParam("messageGuid") UUID messageGuid) {

    reportMessageManager.report(sourceNumber, messageGuid);

    return Response.status(Status.ACCEPTED)
        .build();
  }

  private void sendMessage(Optional<AuthenticatedAccount> source,
      Account destinationAccount,
      Device destinationDevice,
      long timestamp,
      boolean online,
      IncomingMessage incomingMessage)
      throws NoSuchUserException {
    try (final Timer.Context ignored = sendMessageInternalTimer.time()) {
      Optional<byte[]> messageBody = getMessageBody(incomingMessage);
      Optional<byte[]> messageContent = getMessageContent(incomingMessage);
      Envelope.Builder messageBuilder = Envelope.newBuilder();

      messageBuilder.setType(Envelope.Type.forNumber(incomingMessage.getType()))
          .setTimestamp(timestamp == 0 ? System.currentTimeMillis() : timestamp)
          .setServerTimestamp(System.currentTimeMillis());

      if (source.isPresent()) {
        messageBuilder.setSource(source.get().getAccount().getNumber())
            .setSourceUuid(source.get().getAccount().getUuid().toString())
            .setSourceDevice((int) source.get().getAuthenticatedDevice().getId());
      }

      if (messageBody.isPresent()) {
        Metrics.counter(LEGACY_MESSAGE_SENT_COUNTER).increment();
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

  private void sendMessage(Account destinationAccount, Device destinationDevice, long timestamp, boolean online,
      Recipient recipient, byte[] commonPayload) throws NoSuchUserException {
    try (final Timer.Context ignored = sendCommonMessageInternalTimer.time()) {
      Envelope.Builder messageBuilder = Envelope.newBuilder();
      long serverTimestamp = System.currentTimeMillis();
      byte[] recipientKeyMaterial = recipient.getPerRecipientKeyMaterial();

      byte[] payload = new byte[1 + recipientKeyMaterial.length + commonPayload.length];
      payload[0] = MultiRecipientMessageProvider.VERSION;
      System.arraycopy(recipientKeyMaterial, 0, payload, 1, recipientKeyMaterial.length);
      System.arraycopy(commonPayload, 0, payload, 1 + recipientKeyMaterial.length, commonPayload.length);

      messageBuilder
          .setType(Type.UNIDENTIFIED_SENDER)
          .setTimestamp(timestamp == 0 ? serverTimestamp : timestamp)
          .setServerTimestamp(serverTimestamp)
          .setContent(ByteString.copyFrom(payload));

      messageSender.sendMessage(destinationAccount, destinationDevice, messageBuilder.build(), online);
    } catch (NotPushRegisteredException e) {
      if (destinationDevice.isMaster()) {
        throw new NoSuchUserException(e);
      } else {
        logger.debug("Not registered", e);
      }
    }
  }

  @VisibleForTesting
  public static void validateRegistrationIds(Account account, List<IncomingMessage> messages)
      throws StaleDevicesException {
    final Stream<Pair<Long, Integer>> deviceIdAndRegistrationIdStream = messages
        .stream()
        .map(message -> new Pair<>(message.getDestinationDeviceId(), message.getDestinationRegistrationId()));
    validateRegistrationIds(account, deviceIdAndRegistrationIdStream);
  }

  @VisibleForTesting
  public static void validateRegistrationIds(Account account, Stream<Pair<Long, Integer>> deviceIdAndRegistrationIdStream)
      throws StaleDevicesException {
    final List<Long> staleDevices = deviceIdAndRegistrationIdStream
        .filter(deviceIdAndRegistrationId -> deviceIdAndRegistrationId.second() > 0)
        .filter(deviceIdAndRegistrationId -> {
          Optional<Device> device = account.getDevice(deviceIdAndRegistrationId.first());
          return device.isPresent() && deviceIdAndRegistrationId.second() != device.get().getRegistrationId();
        })
        .map(Pair::first)
        .collect(Collectors.toList());

    if (!staleDevices.isEmpty()) {
      throw new StaleDevicesException(staleDevices);
    }
  }

  @VisibleForTesting
  public static void validateCompleteDeviceList(Account account, List<IncomingMessage> messages, boolean isSyncMessage,
      Optional<Long> authenticatedDeviceId)
      throws MismatchedDevicesException {
    Set<Long> messageDeviceIds = messages.stream().map(IncomingMessage::getDestinationDeviceId)
        .collect(Collectors.toSet());
    validateCompleteDeviceList(account, messageDeviceIds, isSyncMessage, authenticatedDeviceId);
  }

  @VisibleForTesting
  public static void validateCompleteDeviceList(Account account, Set<Long> messageDeviceIds, boolean isSyncMessage,
      Optional<Long> authenticatedDeviceId)
      throws MismatchedDevicesException {
    Set<Long> accountDeviceIds = new HashSet<>();

    List<Long> missingDeviceIds = new LinkedList<>();
    List<Long> extraDeviceIds = new LinkedList<>();

    for (Device device : account.getDevices()) {
      if (device.isEnabled() &&
          !(isSyncMessage && device.getId() == authenticatedDeviceId.get())) {
        accountDeviceIds.add(device.getId());

        if (!messageDeviceIds.contains(device.getId())) {
          missingDeviceIds.add(device.getId());
        }
      }
    }

    for (Long deviceId : messageDeviceIds) {
      if (!accountDeviceIds.contains(deviceId)) {
        extraDeviceIds.add(deviceId);
      }
    }

    if (!missingDeviceIds.isEmpty() || !extraDeviceIds.isEmpty()) {
      throw new MismatchedDevicesException(missingDeviceIds, extraDeviceIds);
    }
  }

  private Optional<byte[]> getMessageBody(IncomingMessage message) {
    if (Util.isEmpty(message.getBody())) return Optional.empty();

    try {
      return Optional.of(Base64.getDecoder().decode(message.getBody()));
    } catch (IllegalArgumentException e) {
      logger.debug("Bad B64", e);
      return Optional.empty();
    }
  }

  private Optional<byte[]> getMessageContent(IncomingMessage message) {
    if (Util.isEmpty(message.getContent())) return Optional.empty();

    try {
      return Optional.of(Base64.getDecoder().decode(message.getContent()));
    } catch (IllegalArgumentException e) {
      logger.debug("Bad B64", e);
      return Optional.empty();
    }
  }
}
