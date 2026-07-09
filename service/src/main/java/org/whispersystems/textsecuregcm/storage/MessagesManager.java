/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
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
import org.reactivestreams.Publisher;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MessagesManager {

  final String GET_MESSAGES_FOR_DEVICE_FLUX_NAME = name(MessagesManager.class, "getMessagesForDevice");

  private static final Logger logger = LoggerFactory.getLogger(MessagesManager.class);

  private static final Counter PERSIST_MESSAGE_COUNTER = Metrics.counter(
      name(MessagesManager.class, "persistMessage"));

  private static final Counter PERSIST_MESSAGE_BYTES_COUNTER = Metrics.counter(
      name(MessagesManager.class, "persistMessageBytes"));

  private static final String MAY_HAVE_MESSAGES_COUNTER_NAME =
      MetricsUtil.name(MessagesManager.class, "mayHaveMessages");

  private static final String PRESENCE_MATCH_COUNTER_NAME =
      MetricsUtil.name(MessagesManager.class, "presenceMatch");

  private static final Counter INSERT_TIMEOUT_COUNTER =
      Metrics.counter(name(MessagesManager.class, "insertTimeout"));

  @VisibleForTesting
  static final String MIRROR_INSERTS_EXPERIMENT_NAME = "foundationDbMirrorInserts";

  @VisibleForTesting
  static final String MIRROR_DELETIONS_EXPERIMENT_NAME = "foundationDbMirrorDeletions";

  @VisibleForTesting
  static final String MIRROR_READS_EXPERIMENT_NAME = "foundationDbMirrorReads";

  private static final long FOUNDATIONDB_INSERT_TIMEOUT_MILLIS = Duration.ofSeconds(2).toMillis();

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;
  private final FoundationDbMessageStore foundationDbMessageStore;
  private final RedisMessageAvailabilityManager redisMessageAvailabilityManager;
  private final ReportMessageManager reportMessageManager;
  private final ExecutorService messageDeletionExecutor;
  private final Clock clock;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  public MessagesManager(
      final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final FoundationDbMessageStore foundationDbMessageStore,
      final RedisMessageAvailabilityManager redisMessageAvailabilityManager,
      final ReportMessageManager reportMessageManager,
      final ExecutorService messageDeletionExecutor,
      final Clock clock,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.foundationDbMessageStore = foundationDbMessageStore;
    this.redisMessageAvailabilityManager = redisMessageAvailabilityManager;
    this.reportMessageManager = reportMessageManager;
    this.messageDeletionExecutor = messageDeletionExecutor;
    this.clock = clock;
    this.experimentEnrollmentManager = experimentEnrollmentManager;
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

    final CompletableFuture<Map<Byte, FoundationDbMessageStore.InsertResult>> foundationDbInsertFuture;

    if (experimentEnrollmentManager.isEnrolled(accountIdentifier, MIRROR_INSERTS_EXPERIMENT_NAME)) {
      // Multi-recipient messages will have both a "shared MRM key" and actual message content; we only need/want the
      // latter for FoundationDB
      final Map<Byte, Envelope> minimizedMessagesByDeviceId = messagesByDeviceId.entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(
              Map.Entry::getKey,
              entry -> entry.getValue().toBuilder().clearSharedMrmKey().build()));

      foundationDbInsertFuture =
          foundationDbMessageStore.insert(new AciServiceIdentifier(accountIdentifier), minimizedMessagesByDeviceId)
              .orTimeout(FOUNDATIONDB_INSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
              .exceptionally(e -> {
                if (e instanceof TimeoutException) {
                  INSERT_TIMEOUT_COUNTER.increment();
                } else {
                  logger.warn("Failed to insert {} message(s) for {} into FoundationDB",
                      minimizedMessagesByDeviceId.size(),
                      accountIdentifier,
                      e);
                }

                return Collections.emptyMap();
              });
    } else {
      foundationDbInsertFuture = CompletableFuture.completedFuture(Collections.emptyMap());
    }

    return foundationDbInsertFuture.thenCompose(foundationDbInsertResults ->
            CompletableFuture.allOf(messagesByDeviceId.entrySet().stream()
                .map(deviceIdAndMessage -> {
                  final byte deviceId = deviceIdAndMessage.getKey();

                  // Multi-recipient messages will have both a "shared MRM key" and actual message content; we only
                  // need/want the former for Redis
                  final Envelope message = deviceIdAndMessage.getValue().hasSharedMrmKey()
                      ? deviceIdAndMessage.getValue().toBuilder().clearContent().build()
                      : deviceIdAndMessage.getValue();

                  final UUID messageGuid = Optional.ofNullable(foundationDbInsertResults.get(deviceId))
                      .flatMap(FoundationDbMessageStore.InsertResult::messageGuid)
                      .orElseGet(UUID::randomUUID);

                  return messagesCache.insert(messageGuid, accountIdentifier, deviceId, message)
                      .thenAccept(present -> {
                        if (message.hasSourceServiceId()) {
                          final ServiceIdentifier sourceServiceIdentifier =
                              ServiceIdentifier.fromByteString(message.getSourceServiceId());

                          if (!accountIdentifier.equals(sourceServiceIdentifier.uuid())) {
                            // Note that this is an asynchronous, best-effort, fire-and-forget operation
                            reportMessageManager.store(sourceServiceIdentifier.toServiceIdentifierString(), messageGuid);
                          }
                        }

                        devicePresenceById.put(deviceId, present);

                        if (foundationDbInsertResults.containsKey(deviceId)) {
                          Metrics.counter(PRESENCE_MATCH_COUNTER_NAME,
                                  "match", String.valueOf(present == foundationDbInsertResults.get(deviceId).present()))
                              .increment();
                        }
                      });
                })
                .toArray(CompletableFuture[]::new)))
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
          final Envelope.Builder envelopeBuilder = Envelope.newBuilder()
              .setType(Envelope.Type.UNIDENTIFIED_SENDER)
              .setClientTimestamp(clientTimestamp == 0 ? serverTimestamp : clientTimestamp)
              .setServerTimestamp(serverTimestamp)
              .setEphemeral(isEphemeral)
              .setUrgent(isUrgent)
              .setSharedMrmKey(ByteString.copyFrom(sharedMrmKey));

          if (isStory) {
            // Avoid sending this field if it's false.
            envelopeBuilder.setStory(true);
          }

          final Envelope prototypeMessage = envelopeBuilder.build();

          final Map<Account, Map<Byte, Boolean>> clientPresenceByAccountAndDevice = new ConcurrentHashMap<>();

          return CompletableFuture.allOf(multiRecipientMessage.getRecipients().entrySet().stream()
                  .filter(serviceIdAndRecipient -> resolvedRecipients.containsKey(serviceIdAndRecipient.getValue()))
                  .map(serviceIdAndRecipient -> {
                    final ServiceIdentifier serviceIdentifier = ServiceIdentifier.fromLibsignal(serviceIdAndRecipient.getKey());
                    final SealedSenderMultiRecipientMessage.Recipient recipient = serviceIdAndRecipient.getValue();
                    final byte[] devices = recipient.getDevices();

                    return insertAsync(resolvedRecipients.get(recipient).getIdentifier(IdentityType.ACI),
                        IntStream.range(0, devices.length).mapToObj(i -> devices[i])
                            .collect(Collectors.toMap(deviceId -> deviceId, _ -> prototypeMessage.toBuilder()
                                .setContent(ByteString.copyFrom(multiRecipientMessage.messageForRecipient(serviceIdAndRecipient.getValue())))
                                .setDestinationServiceId(serviceIdentifier.toCompactByteString())
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

  public MessageStream getMessages(final UUID destinationUuid, final Device destinationDevice) {
    final RedisDynamoDbMessageStream redisDynamoDbMessageStream =
        new RedisDynamoDbMessageStream(messagesDynamoDb, messagesCache, redisMessageAvailabilityManager,
            destinationUuid, destinationDevice);

    return new MirroringMessageStream(
        redisDynamoDbMessageStream,
        foundationDbMessageStore.getMessages(new AciServiceIdentifier(destinationUuid), destinationDevice.getId()),
        experimentEnrollmentManager,
        destinationUuid,
        destinationDevice.getId());
  }

  Publisher<Envelope> getMessagesForDevice(final UUID destinationUuid, final Device destinationDevice) {
    return Flux.concat(
            messagesDynamoDb.load(destinationUuid, destinationDevice, null),
            messagesCache.get(destinationUuid, destinationDevice.getId()))
        .name(GET_MESSAGES_FOR_DEVICE_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry));
  }

  public CompletableFuture<Void> clear(final UUID destinationUuid) {
    if (experimentEnrollmentManager.isEnrolled(destinationUuid, MessagesManager.MIRROR_DELETIONS_EXPERIMENT_NAME)) {
      messageDeletionExecutor.execute(() -> {
        try {
          foundationDbMessageStore.clearAll(new AciServiceIdentifier(destinationUuid));
        } catch (final Exception e) {
          logger.warn("Failed to clear messages for {}", destinationUuid, e);
        }
      });
    }

    return messagesCache.clear(destinationUuid);
  }

  public CompletableFuture<Void> clear(final UUID destinationUuid, final byte deviceId) {
    if (experimentEnrollmentManager.isEnrolled(destinationUuid, MessagesManager.MIRROR_DELETIONS_EXPERIMENT_NAME)) {
      messageDeletionExecutor.execute(() -> {
        try {
          foundationDbMessageStore.clearAll(new AciServiceIdentifier(destinationUuid), deviceId);
        } catch (final Exception e) {
          logger.warn("Failed to clear messages for {}:{}", destinationUuid, deviceId, e);
        }
      });
    }

    return messagesCache.clear(destinationUuid, deviceId);
  }

  public CompletableFuture<Optional<RemovedMessage>> delete(final UUID destinationUuid,
      final Device destinationDevice,
      final UUID guid,
      final long serverTimestamp) {

    return messagesCache.remove(destinationUuid, destinationDevice.getId(), guid)
        .thenComposeAsync(removed -> removed
            .map(_ -> CompletableFuture.completedFuture(removed))
            .orElseGet(() -> messagesDynamoDb.deleteMessage(destinationUuid, destinationDevice, guid, serverTimestamp)
                .thenApply(maybeEnvelope -> maybeEnvelope.map(RemovedMessage::fromEnvelope))
            ), messageDeletionExecutor);
  }

  /**
   * @return the number of messages successfully removed from the cache.
   */
  public int persistMessages(
      final UUID destinationUuid,
      final Device destinationDevice,
      final List<Envelope> messages) {

    messagesDynamoDb.store(messages, destinationUuid, destinationDevice);

    final List<UUID> messageGuids = messages.stream().map(message -> UUIDUtil.fromByteString(message.getServerGuid()))
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

  /// Record versionstamps for the current time in the FoundationDB database(s).
  public void recordFoundationDbVersionstamps() {
    foundationDbMessageStore.recordVersionstamps();
  }

  /// Clean up expired entries in the FoundationDB versionstamp clock.
  ///
  /// @param oldestRetainedEntryTimestamp the earliest time for which we still want to be able to obtain a versionstamp
  public void expireOldFoundationDbVersionstamps(final Instant oldestRetainedEntryTimestamp) {
    foundationDbMessageStore.expireOldVersionstamps(oldestRetainedEntryTimestamp);
  }

  /// Delete messages in FoundationDB for the given devices that were inserted before the given
  /// time. (Expiration of messages from Redis and Dynamo is handled automatically and do not
  /// require any interaction with this class.)
  ///
  /// @param accountDeviceIdentifiers a map from ACI to the list of device IDs for which expired messages should be
  /// trimmed.
  ///
  /// @param cutoffTime the expiration threshold. Messages inserted before this time may be deleted; messages inserted
  /// after it will not be.
  public void deleteFoundationDbMessagesBefore(final Map<AciServiceIdentifier, List<Byte>> accountDeviceIdentifiers, final Instant cutoffTime) {
    foundationDbMessageStore.deleteMessagesBefore(accountDeviceIdentifiers, cutoffTime);
  }
}
