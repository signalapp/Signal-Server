/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.FDBException;
import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.core.publisher.Mono;

public class MessagesManager {

  private static final Logger logger = LoggerFactory.getLogger(MessagesManager.class);

  private final MessagesDynamoDb messagesDynamoDb;
  private final FoundationDbMessageStore foundationDbMessageStore;
  private final ReportMessageManager reportMessageManager;
  private final ExecutorService messageDeletionExecutor;
  private final Clock clock;

  public MessagesManager(
      final MessagesDynamoDb messagesDynamoDb,
      final FoundationDbMessageStore foundationDbMessageStore,
      final ReportMessageManager reportMessageManager,
      final ExecutorService messageDeletionExecutor,
      final Clock clock) {

    this.messagesDynamoDb = messagesDynamoDb;
    this.foundationDbMessageStore = foundationDbMessageStore;
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
   * @return a map of device IDs to a device's presence state
   */
  public Map<Byte, Boolean> insert(final UUID accountIdentifier, final Map<Byte, Envelope> messagesByDeviceId) {
    return insertAsync(accountIdentifier, messagesByDeviceId).join();
  }

  private CompletableFuture<Map<Byte, Boolean>> insertAsync(final UUID accountIdentifier, final Map<Byte, Envelope> messagesByDeviceId) {
    return foundationDbMessageStore.insert(new AciServiceIdentifier(accountIdentifier), messagesByDeviceId)
        .thenApply(insertResultsByDeviceId -> {
          messagesByDeviceId.forEach((deviceId, message) -> {
            if (message.hasSourceServiceId()) {
              final ServiceIdentifier sourceServiceIdentifier =
                  ServiceIdentifier.fromByteString(message.getSourceServiceId());

              if (!accountIdentifier.equals(sourceServiceIdentifier.uuid()) && message.getType() != Envelope.Type.SERVER_DELIVERY_RECEIPT) {
                insertResultsByDeviceId.get(deviceId).messageGuid().ifPresent(messageGuid -> {
                  // Note that this is an asynchronous, best-effort, fire-and-forget operation
                  reportMessageManager.store(sourceServiceIdentifier.toServiceIdentifierString(), messageGuid);
                });
              }
            }
          });

          return insertResultsByDeviceId.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().present()));
        })
        .whenComplete((_, throwable) -> {
          if (throwable == null) {
            return;
          }

          if (!(ExceptionUtils.unwrap(throwable) instanceof FDBException) && !ExceptionUtils.isBreakerOpen(throwable)) {
            logger.warn("Failed to insert {} message(s) for {} into FoundationDB",
                messagesByDeviceId.size(),
                accountIdentifier,
                throwable);
          }
        });
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
   * @return a map of accounts to maps of device IDs to a device's presence state
   */
  public CompletableFuture<Map<Account, Map<Byte, Boolean>>> insertMultiRecipientMessage(
      final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients,
      final long clientTimestamp,
      final boolean isStory,
      final boolean isEphemeral,
      final boolean isUrgent) {

    final long serverTimestamp = clock.millis();

    final Envelope.Builder envelopeBuilder = Envelope.newBuilder()
        .setType(Envelope.Type.UNIDENTIFIED_SENDER)
        .setClientTimestamp(clientTimestamp == 0 ? serverTimestamp : clientTimestamp)
        .setServerTimestamp(serverTimestamp)
        .setEphemeral(isEphemeral)
        .setUrgent(isUrgent);

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

              return insertAsync(resolvedRecipients.get(recipient).getAccountIdentifier(),
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
  }

  public CompletableFuture<Boolean> mayHavePersistedMessages(final UUID destinationUuid, final Device destinationDevice) {
    return foundationDbMessageStore.hasMessages(new AciServiceIdentifier(destinationUuid), destinationDevice.getId())
        .thenCombine(messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice), (a, b) -> a || b);
  }

  public CompletableFuture<Boolean> mayHaveMessages(final UUID destinationUuid, final Device destinationDevice) {
    @SuppressWarnings("unchecked") final CompletableFuture<Boolean>[] mayHaveMessagesFutures = new CompletableFuture[] {
        foundationDbMessageStore.hasMessages(new AciServiceIdentifier(destinationUuid), destinationDevice.getId()),
        messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice)
    };

    return CompletableFuture.allOf(mayHaveMessagesFutures)
        .thenApply(_ -> Arrays.stream(mayHaveMessagesFutures).anyMatch(CompletableFuture::resultNow));
  }

  public CompletableFuture<Boolean> mayHaveUrgentPersistedMessages(final UUID destinationUuid, final Device destinationDevice) {
    return foundationDbMessageStore.mayHaveUrgentMessages(new AciServiceIdentifier(destinationUuid), destinationDevice.getId())
        .thenCombine(messagesDynamoDb.mayHaveUrgentMessages(destinationUuid, destinationDevice), (a, b) -> a || b);
  }

  public MessageStream getMessages(final UUID destinationUuid, final Device destinationDevice) {
    return new ConcatenatingMessageStream(
        new DynamoDbMessageStream(messagesDynamoDb, destinationUuid, destinationDevice),
        foundationDbMessageStore.getMessages(new AciServiceIdentifier(destinationUuid), destinationDevice.getId())
    );
  }

  public CompletableFuture<Void> clear(final UUID destinationUuid) {
    return CompletableFuture.runAsync(() -> {
      try {
        foundationDbMessageStore.clearAll(new AciServiceIdentifier(destinationUuid));
      } catch (final Exception e) {
        if (!ExceptionUtils.isBreakerOpen(e)) {
          logger.warn("Failed to clear messages for {}", destinationUuid, e);
        }
      }
    }, messageDeletionExecutor);
  }

  public CompletableFuture<Void> clear(final UUID destinationUuid, final byte deviceId) {
    return CompletableFuture.runAsync(() -> {
      try {
        foundationDbMessageStore.clearAll(new AciServiceIdentifier(destinationUuid), deviceId);
      } catch (final Exception e) {
        if (!ExceptionUtils.isBreakerOpen(e)) {
          logger.warn("Failed to clear messages for {}:{}", destinationUuid, deviceId, e);
        }
      }
    }, messageDeletionExecutor);
  }

  public CompletableFuture<Optional<Instant>> getEarliestUndeliveredTimestampForDevice(UUID destinationUuid, Device destinationDevice) {
    // If there's any message in the persisted layer, return the oldest
    return Mono.from(messagesDynamoDb.load(destinationUuid, destinationDevice, 1)).map(Envelope::getServerTimestamp)
        // And if there are none there, then check FoundationDB
        .switchIfEmpty(Mono.fromFuture(() -> foundationDbMessageStore.getEarliestUndeliveredMessageTimestamp(new AciServiceIdentifier(destinationUuid), destinationDevice.getId()))
            .mapNotNull(maybeTimestamp -> maybeTimestamp.map(Instant::toEpochMilli).orElse(null)))
        .map(epochMilli -> Optional.of(Instant.ofEpochMilli(epochMilli)))
        .switchIfEmpty(Mono.just(Optional.empty()))
        .toFuture();
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

  /// Trim the provided message queue if needed.
  ///
  /// @param aci The ACI of the message queue to trim
  /// @param device The device of the message queue to trim
  /// @param maxQueueSizeBytes The threshold size beyond which the message queue will be trimmed
  /// @param targetQueueSizeBytes The target size to trim to
  /// @param rangeSplitChunkSizeBytes The approximate chunk size in bytes used to split the range covered by the message queue
  /// @param dryRun Whether to actually trim the provided queue
  public Mono<Void> trimQueue(
      final AciServiceIdentifier aci,
      final Device device,
      final long maxQueueSizeBytes,
      final long targetQueueSizeBytes,
      final long rangeSplitChunkSizeBytes,
      final boolean dryRun) {
    // All trims will be considered successful during a dry run
    final UnaryOperator<Mono<Boolean>> deleteModifier = dryRun ? _ -> Mono.just(true) : UnaryOperator.identity();
    return foundationDbMessageStore.trimQueue(aci, device, maxQueueSizeBytes, targetQueueSizeBytes, rangeSplitChunkSizeBytes, deleteModifier);
  }
}
