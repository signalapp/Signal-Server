/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.util.Pair;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MessagesManager {

  private static final int RESULT_SET_CHUNK_SIZE = 100;
  final String GET_MESSAGES_FOR_DEVICE_FLUX_NAME = name(MessagesManager.class, "getMessagesForDevice");

  private static final Logger logger = LoggerFactory.getLogger(MessagesManager.class);

  private static final Counter PERSIST_MESSAGE_COUNTER = Metrics.counter(
      name(MessagesManager.class, "persistMessage"));

  private static final Counter PERSIST_MESSAGE_BYTES_COUNTER = Metrics.counter(
      name(MessagesManager.class, "persistMessageBytes"));

  private static final String MAY_HAVE_MESSAGES_COUNTER_NAME =
      MetricsUtil.name(MessagesManager.class, "mayHaveMessages");

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;
  private final RedisMessageAvailabilityManager redisMessageAvailabilityManager;
  private final ReportMessageManager reportMessageManager;
  private final ExecutorService messageDeletionExecutor;
  private final Clock clock;

  public MessagesManager(
      final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final RedisMessageAvailabilityManager redisMessageAvailabilityManager,
      final ReportMessageManager reportMessageManager,
      final ExecutorService messageDeletionExecutor,
      final Clock clock) {

    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.redisMessageAvailabilityManager = redisMessageAvailabilityManager;
    this.reportMessageManager = reportMessageManager;
    this.messageDeletionExecutor = messageDeletionExecutor;
    this.clock = clock;
  }

  /**
   * Inserts messages into the message queues for devices associated with the identified account.
   *
   * @param accountIdentifier the account identifier for the destination queue
   * @param messagesByDeviceId a map of device IDs to messages
   *
   * @return a map of device IDs to a device's presence state (i.e. if the device has an active event listener)
   *
   * @see RedisMessageAvailabilityManager
   */
  public Map<Byte, Boolean> insert(final UUID accountIdentifier, final Map<Byte, Envelope> messagesByDeviceId) {
    return insertAsync(accountIdentifier, messagesByDeviceId).join();
  }

  private CompletableFuture<Map<Byte, Boolean>> insertAsync(final UUID accountIdentifier, final Map<Byte, Envelope> messagesByDeviceId) {
    final Map<Byte, Boolean> devicePresenceById = new ConcurrentHashMap<>();

    return CompletableFuture.allOf(messagesByDeviceId.entrySet().stream()
            .map(deviceIdAndMessage -> {
              final byte deviceId = deviceIdAndMessage.getKey();
              final Envelope message = deviceIdAndMessage.getValue();
              final UUID messageGuid = UUID.randomUUID();

              return messagesCache.insert(messageGuid, accountIdentifier, deviceId, message)
                  .thenAccept(present -> {
                    if (message.hasSourceServiceId() && !accountIdentifier.toString()
                        .equals(message.getSourceServiceId())) {
                      // Note that this is an asynchronous, best-effort, fire-and-forget operation
                      reportMessageManager.store(message.getSourceServiceId(), messageGuid);
                    }

                    devicePresenceById.put(deviceId, present);
                  });
            })
            .toArray(CompletableFuture[]::new))
        .thenApply(ignored -> devicePresenceById);
  }

  /**
   * Inserts messages into the message queues for devices associated with the identified accounts.
   *
   * @param multiRecipientMessage the multi-recipient message to insert into destination queues
   * @param resolvedRecipients    a map of multi-recipient message {@code Recipient} entities to their corresponding
   *                              Signal accounts; messages will not be delivered to unresolved recipients
   * @param clientTimestamp       the timestamp for the message as reported by the sending party
   * @param isStory               {@code true} if the given message is a story or {@code false} otherwise
   * @param isEphemeral           {@code true} if the given message should only be delivered to devices with active
   *                              connections to a Signal server or {@code false} otherwise
   * @param isUrgent              {@code true} if the given message is urgent or {@code false} otherwise
   *
   * @return a map of accounts to maps of device IDs to a device's presence state (i.e. if the device has an active
   * event listener)
   *
   * @see RedisMessageAvailabilityManager
   */
  public CompletableFuture<Map<Account, Map<Byte, Boolean>>> insertMultiRecipientMessage(
      final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients,
      final long clientTimestamp,
      final boolean isStory,
      final boolean isEphemeral,
      final boolean isUrgent) {

    final long serverTimestamp = clock.millis();

    return insertSharedMultiRecipientMessagePayload(multiRecipientMessage)
        .thenCompose(sharedMrmKey -> {
          final Envelope prototypeMessage = Envelope.newBuilder()
              .setType(Envelope.Type.UNIDENTIFIED_SENDER)
              .setClientTimestamp(clientTimestamp == 0 ? serverTimestamp : clientTimestamp)
              .setServerTimestamp(serverTimestamp)
              .setStory(isStory)
              .setEphemeral(isEphemeral)
              .setUrgent(isUrgent)
              .setSharedMrmKey(ByteString.copyFrom(sharedMrmKey))
              .build();

          final Map<Account, Map<Byte, Boolean>> clientPresenceByAccountAndDevice = new ConcurrentHashMap<>();

          return CompletableFuture.allOf(multiRecipientMessage.getRecipients().entrySet().stream()
                  .filter(serviceIdAndRecipient -> resolvedRecipients.containsKey(serviceIdAndRecipient.getValue()))
                  .map(serviceIdAndRecipient -> {
                    final ServiceIdentifier serviceIdentifier = ServiceIdentifier.fromLibsignal(serviceIdAndRecipient.getKey());
                    final SealedSenderMultiRecipientMessage.Recipient recipient = serviceIdAndRecipient.getValue();
                    final byte[] devices = recipient.getDevices();

                    return insertAsync(resolvedRecipients.get(recipient).getIdentifier(IdentityType.ACI),
                        IntStream.range(0, devices.length).mapToObj(i -> devices[i])
                            .collect(Collectors.toMap(deviceId -> deviceId, deviceId -> prototypeMessage.toBuilder()
                                .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
                                .build())))
                        .thenAccept(clientPresenceByDeviceId ->
                            clientPresenceByAccountAndDevice.put(resolvedRecipients.get(recipient),
                                clientPresenceByDeviceId));
                  })
                  .toArray(CompletableFuture[]::new))
              .thenApply(ignored -> clientPresenceByAccountAndDevice);
        });
  }

  public CompletableFuture<Boolean> mayHavePersistedMessages(final UUID destinationUuid, final Device destinationDevice) {
    return messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice);
  }

  public CompletableFuture<Boolean> mayHaveMessages(final UUID destinationUuid, final Device destinationDevice) {
    return messagesCache.hasMessagesAsync(destinationUuid, destinationDevice.getId())
        .thenCombine(messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice),
            (mayHaveCachedMessages, mayHavePersistedMessages) -> {
              final String outcome;

              if (mayHaveCachedMessages && mayHavePersistedMessages) {
                outcome = "both";
              } else if (mayHaveCachedMessages) {
                outcome = "cached";
              } else if (mayHavePersistedMessages) {
                outcome = "persisted";
              } else {
                outcome = "none";
              }

              Metrics.counter(MAY_HAVE_MESSAGES_COUNTER_NAME, "outcome", outcome).increment();

              return mayHaveCachedMessages || mayHavePersistedMessages;
            });
  }

  public CompletableFuture<Boolean> mayHaveUrgentPersistedMessages(final UUID destinationUuid, final Device destinationDevice) {
    return messagesDynamoDb.mayHaveUrgentMessages(destinationUuid, destinationDevice);
  }

  public Mono<Pair<List<Envelope>, Boolean>> getMessagesForDevice(UUID destinationUuid, Device destinationDevice,
      boolean cachedMessagesOnly) {

    return Flux.from(
            getMessagesForDevice(destinationUuid, destinationDevice, RESULT_SET_CHUNK_SIZE, cachedMessagesOnly))
        .take(RESULT_SET_CHUNK_SIZE)
        .collectList()
        .map(envelopes -> new Pair<>(envelopes, envelopes.size() >= RESULT_SET_CHUNK_SIZE));
  }

  public Publisher<Envelope> getMessagesForDeviceReactive(UUID destinationUuid, Device destinationDevice,
      final boolean cachedMessagesOnly) {

    return getMessagesForDevice(destinationUuid, destinationDevice, null, cachedMessagesOnly);
  }

  public MessageStream getMessages(final UUID destinationUuid, final Device destinationDevice) {
    return new RedisDynamoDbMessageStream(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager, destinationUuid, destinationDevice);
  }

  private Publisher<Envelope> getMessagesForDevice(UUID destinationUuid, Device destinationDevice,
      @Nullable Integer limit, final boolean cachedMessagesOnly) {

    final Publisher<Envelope> dynamoPublisher =
        cachedMessagesOnly ? Flux.empty() : messagesDynamoDb.load(destinationUuid, destinationDevice, limit);
    final Publisher<Envelope> cachePublisher = messagesCache.get(destinationUuid, destinationDevice.getId());

    return Flux.concat(dynamoPublisher, cachePublisher)
        .name(GET_MESSAGES_FOR_DEVICE_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry));
  }

  public CompletableFuture<Void> clear(UUID destinationUuid) {
    return messagesCache.clear(destinationUuid);
  }

  public CompletableFuture<Void> clear(UUID destinationUuid, byte deviceId) {
    return messagesCache.clear(destinationUuid, deviceId);
  }

  public CompletableFuture<Optional<RemovedMessage>> delete(UUID destinationUuid, Device destinationDevice, UUID guid,
      @Nullable Long serverTimestamp) {
    return messagesCache.remove(destinationUuid, destinationDevice.getId(), guid)
        .thenComposeAsync(removed -> {

          if (removed.isPresent()) {
            return CompletableFuture.completedFuture(removed);
          }

          final CompletableFuture<Optional<MessageProtos.Envelope>> maybeDeletedEnvelope;
          if (serverTimestamp == null) {
            maybeDeletedEnvelope = messagesDynamoDb.deleteMessageByDestinationAndGuid(destinationUuid,
                destinationDevice, guid);
          } else {
            maybeDeletedEnvelope = messagesDynamoDb.deleteMessage(destinationUuid, destinationDevice, guid,
                serverTimestamp);
          }

          return maybeDeletedEnvelope.thenApply(maybeEnvelope -> maybeEnvelope.map(RemovedMessage::fromEnvelope));
        }, messageDeletionExecutor);
  }

  /**
   * @return the number of messages successfully removed from the cache.
   */
  public int persistMessages(
      final UUID destinationUuid,
      final Device destinationDevice,
      final List<Envelope> messages) {

    messagesDynamoDb.store(messages, destinationUuid, destinationDevice);

    final List<UUID> messageGuids = messages.stream().map(message -> UUID.fromString(message.getServerGuid()))
        .collect(Collectors.toList());
    int messagesRemovedFromCache = 0;
    try {
      messagesRemovedFromCache = messagesCache.remove(destinationUuid, destinationDevice.getId(), messageGuids)
          .get(30, TimeUnit.SECONDS).size();
      PERSIST_MESSAGE_COUNTER.increment(messages.size());
      PERSIST_MESSAGE_BYTES_COUNTER.increment(messages.stream()
          .mapToInt(Envelope::getSerializedSize)
          .sum());

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Failed to remove messages from cache", e);
    }
    return messagesRemovedFromCache;
  }

  public CompletableFuture<Optional<Instant>> getEarliestUndeliveredTimestampForDevice(UUID destinationUuid, Device destinationDevice) {
    // If there's any message in the persisted layer, return the oldest
    return Mono.from(messagesDynamoDb.load(destinationUuid, destinationDevice, 1)).map(Envelope::getServerTimestamp)
        // If not, return the oldest message in the cache
        .switchIfEmpty(messagesCache.getEarliestUndeliveredTimestamp(destinationUuid, destinationDevice.getId()))
        .map(epochMilli -> Optional.of(Instant.ofEpochMilli(epochMilli)))
        .switchIfEmpty(Mono.just(Optional.empty()))
        .toFuture();
  }

  /**
   * Inserts the shared multi-recipient message payload to storage.
   *
   * @return a key where the shared data is stored
   * @see MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript
   */
  private CompletableFuture<byte[]> insertSharedMultiRecipientMessagePayload(
      final SealedSenderMultiRecipientMessage sealedSenderMultiRecipientMessage) {
    return messagesCache.insertSharedMultiRecipientMessagePayload(sealedSenderMultiRecipientMessage);
  }
}
