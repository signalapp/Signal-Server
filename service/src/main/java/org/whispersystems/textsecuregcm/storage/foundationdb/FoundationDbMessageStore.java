package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;

/// An implementation of a message store backed by FoundationDB.
///
/// @implNote The layout of elements in FoundationDB is as follows:
/// * messages
///   * {aci}
///     * messageAvailableWatch => versionstamp
///     * {deviceId}
///       * presence => server_id | last_seen_seconds_since_epoch
///       * queue
///         * {versionstamp_1} => envelope_1
///         * {versionstamp_2} => envelope_2
public class FoundationDbMessageStore {

  private final Database[][] databasesByEpoch;
  private final Map<Database, VersionstampClock> versionstampClocks;
  private final int[] liveEpochs;
  private final int activeEpoch;
  private final VersionstampUUIDCipher versionstampUUIDCipher;
  private final ScheduledExecutorService presenceRenewalExecutorService;
  private final Clock clock;

  private static final byte[] SERVER_ID = UUIDUtil.toBytes(UUID.randomUUID());
  private static final int PRESENCE_VALUE_LENGTH = 28;

  private static final Subspace MESSAGES_SUBSPACE = new Subspace(Tuple.from("M"));

  @VisibleForTesting
  static final Duration PRESENCE_STALE_THRESHOLD = Duration.ofMinutes(5);

  /// The (approximate) transaction size beyond which we do not add more messages in a transaction. The estimated size
  /// includes only message payloads (and not key reads/writes) which we assume will dominate the total
  /// transaction size. Note that the FDB [docs](https://apple.github.io/foundationdb/known-limitations.html) currently
  /// suggest a limit of 1MB to avoid performance issues, although the hard limit is 10MB
  private static final long MAX_MESSAGE_CHUNK_SIZE = DataSize.megabytes(1).toBytes();

  // We pack the current configuration epoch and shard ID into a single byte of "user data" in each message
  // versionstamp. We use two bits for the epoch and six for the shard ID.
  public static final int MAX_EPOCHS = 4;
  public static final int MAX_SHARDS = 64;

  private static final Counter INSERT_MESSAGE_COUNTER =
      Metrics.counter(MetricsUtil.name(FoundationDbMessageStore.class, "insertMessage"));

  private static final Timer INSERT_MESSAGE_BATCH_TIMER =
      Metrics.timer(MetricsUtil.name(FoundationDbMessageStore.class, "insertMessageBatchTimer"));

  private static final Counter DELETE_MESSAGE_COUNTER =
      Metrics.counter(MetricsUtil.name(FoundationDbMessageStore.class, "deleteMessage"));

  private static final Timer DELETE_MESSAGE_TIMER =
      Metrics.timer(MetricsUtil.name(FoundationDbMessageStore.class, "deleteMessageTimer"));

  /// Result of inserting a message for a particular device
  ///
  /// @param versionstamp the versionstamp of the transaction in which this device's message was inserted, empty
  ///                     otherwise
  /// @param messageGuid  the versionstamp encrypted/encoded as a version 8 UUID
  /// @param present      whether the device is online
  public record InsertResult(Optional<Versionstamp> versionstamp,
                             Optional<UUID> messageGuid,
                             boolean present) {
  }

  public FoundationDbMessageStore(final Map<Integer, List<Database>> databasesByEpochMap,
      final int activeEpoch,
      final VersionstampUUIDCipher versionstampUUIDCipher,
      final ScheduledExecutorService presenceRenewalExecutorService,
      final Clock clock) {

    final Database[][] databasesByEpochArray = new Database[MAX_EPOCHS][];

    databasesByEpochMap.forEach((epoch, databases) ->
        databasesByEpochArray[epoch] = databases.toArray(Database[]::new));

    this.databasesByEpoch = databasesByEpochArray;
    this.liveEpochs = IntStream.range(0, MAX_EPOCHS).filter(e -> databasesByEpochArray[e] != null).toArray();
    this.activeEpoch = activeEpoch;
    this.versionstampUUIDCipher = versionstampUUIDCipher;
    this.presenceRenewalExecutorService = presenceRenewalExecutorService;
    this.versionstampClocks = databasesByEpochMap.values().stream()
        .flatMap(List::stream)
        .distinct()
        .collect(Collectors.toMap(Function.identity(),
            db -> new VersionstampClock(db, clock),
            (_, _) -> {
              throw new AssertionError("Source stream had duplicates after distinct()");
            },
            IdentityHashMap::new));
    this.clock = clock;
  }

  /// Convenience method for inserting a single recipient message bundle. See [#insert(Map)] for details.
  ///
  /// @param aciServiceIdentifier accountId of the recipient
  /// @param messagesByDeviceId   a map of message envelopes by deviceId to be inserted
  /// @return a future that yields a map deviceId => the presence state and versionstamp of the transaction in which the
  /// device's message was inserted (if any)
  public CompletableFuture<Map<Byte, InsertResult>> insert(final AciServiceIdentifier aciServiceIdentifier,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId) {

    return insert(aciServiceIdentifier, messagesByDeviceId, activeEpoch);
  }

  @VisibleForTesting
  CompletableFuture<Map<Byte, InsertResult>> insert(final AciServiceIdentifier aciServiceIdentifier,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId,
      final int epoch) {

    return insert(Map.of(aciServiceIdentifier, messagesByDeviceId), epoch)
        .thenApply(resultsByServiceIdentifier -> {
          assert resultsByServiceIdentifier.size() == 1;

          return resultsByServiceIdentifier.get(aciServiceIdentifier);
        });
  }

  /// Insert a multi-recipient message bundle. Destination ACIs are grouped by shard number. Each shard then starts a
  /// potentially multi-transaction operation. Messages are inserted in chunks to avoid transaction size limits.
  ///
  /// @param messagesByServiceIdentifier a map of accountId to message envelopes by deviceId
  /// @return a future that yields a map containing the presence states of devices and versionstamps corresponding to
  /// committed transactions during this operation
  ///
  /// @implNote All messages belonging to the same recipient are always committed in the same transaction for
  /// simplicity. A message may not be inserted if the device is not present (as determined from its presence key) and
  /// the message is ephemeral. If no messages in a transaction end up being inserted, we won't commit it since the
  /// transaction was read-only. As such, no corresponding versionstamp is generated.
  public CompletableFuture<Map<AciServiceIdentifier, Map<Byte, InsertResult>>> insert(
      final Map<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>> messagesByServiceIdentifier) {

    return insert(messagesByServiceIdentifier, activeEpoch);
  }

  @VisibleForTesting
  CompletableFuture<Map<AciServiceIdentifier, Map<Byte, InsertResult>>> insert(
      final Map<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>> messagesByServiceIdentifier,
      final int epoch) {

    final Timer.Sample sample = Timer.start();

    if (messagesByServiceIdentifier.entrySet()
        .stream()
        .anyMatch(entry -> entry.getValue().isEmpty())) {
      throw new IllegalArgumentException("One or more message bundles is empty");
    }

    if (messagesByServiceIdentifier.values()
        .stream()
        .flatMap(messages -> messages.values().stream())
        .anyMatch(MessageProtos.Envelope::hasServerGuid)) {

      throw new IllegalArgumentException("Messages must not have pre-set server GUIDs");
    }

    final Map<Integer, List<Map.Entry<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>>>> messagesByShardId =
        messagesByServiceIdentifier.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> hashAciToShardNumber(entry.getKey(), epoch)));

    final List<CompletableFuture<Map<AciServiceIdentifier, Map<Byte, InsertResult>>>> chunkFutures =
        new ArrayList<>();

    messagesByShardId.forEach((shardId, messagesForShard) -> {
      int start = 0, current = 0;
      int estimatedTransactionSize = 0;

      while (current < messagesForShard.size()) {
        estimatedTransactionSize += messagesForShard.get(current).getValue().values()
            .stream()
            .mapToInt(MessageProtos.Envelope::getSerializedSize)
            .sum();

        if (estimatedTransactionSize > MAX_MESSAGE_CHUNK_SIZE) {
          chunkFutures.add(insertChunk(shardId, epoch, messagesForShard.subList(start, current)));

          start = current;
          estimatedTransactionSize = 0;
        } else {
          current++;
        }
      }

      assert start < messagesForShard.size();
      chunkFutures.add(insertChunk(shardId, epoch, messagesForShard.subList(start, messagesForShard.size())));
    });

    return CompletableFuture.allOf(chunkFutures.toArray(CompletableFuture[]::new))
        .thenApply(_ -> chunkFutures.stream()
            .map(CompletableFuture::join)
            .reduce(new HashMap<>(), (a, b) -> {
              a.putAll(b);
              return a;
            }))
        .whenComplete((_, throwable) -> {
          if (throwable == null) {
            sample.stop(INSERT_MESSAGE_BATCH_TIMER);
            INSERT_MESSAGE_COUNTER.increment(messagesByServiceIdentifier.values().stream().mapToInt(Map::size).sum());
          }
        });
  }

  private CompletableFuture<Map<AciServiceIdentifier, Map<Byte, InsertResult>>> insertChunk(
      final int shardId,
      final int epoch,
      final List<Map.Entry<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>>> messagesByAccountIdentifier) {

    final Map<AciServiceIdentifier, CompletableFuture<Map<Byte, Boolean>>> insertFuturesByAci = new HashMap<>();

    // In a message bundle (single-recipient or MRM) the ephemerality should be the same for all envelopes, so just get the first.
    final boolean ephemeral = messagesByAccountIdentifier.stream()
        .findFirst()
        .flatMap(entry -> entry.getValue().values().stream().findFirst())
        .map(MessageProtos.Envelope::getEphemeral)
        .orElseThrow(() -> new IllegalStateException("One or more bundles is empty"));

    return getDatabases(epoch)[shardId].runAsync(transaction -> {
          messagesByAccountIdentifier.forEach(entry ->
              insertFuturesByAci.put(entry.getKey(), insert(entry.getKey(), entry.getValue(), epoch, shardId, transaction)));

          return CompletableFuture.allOf(insertFuturesByAci.values().toArray(CompletableFuture[]::new))
              .thenApply(_ -> {
                final boolean anyClientPresent = insertFuturesByAci.values()
                    .stream()
                    .map(CompletableFuture::join)
                    .flatMap(presenceByDeviceId -> presenceByDeviceId.values().stream())
                    .anyMatch(isPresent -> isPresent);
                if (anyClientPresent || !ephemeral) {
                  return transaction.getVersionstamp()
                      .thenApply(versionstampBytes -> Optional.of(Versionstamp.complete(versionstampBytes,
                          packUserData(epoch, shardId))));
                }
                return CompletableFuture.completedFuture(Optional.<Versionstamp>empty());
              });
        })
        .thenCompose(Function.identity())
        .thenApply(maybeVersionstamp -> insertFuturesByAci.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
              assert entry.getValue().isDone();
              final Map<Byte, Boolean> presenceByDeviceId = entry.getValue().join();

              return presenceByDeviceId.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, presenceEntry -> {
                    final Optional<Versionstamp> insertResultVersionstamp;
                    if (presenceEntry.getValue() || !ephemeral) {
                      assert maybeVersionstamp.isPresent();
                      insertResultVersionstamp = maybeVersionstamp;
                    } else {
                      insertResultVersionstamp = Optional.empty();
                    }

                    return new InsertResult(insertResultVersionstamp,
                        insertResultVersionstamp.map(versionstamp ->
                            versionstampUUIDCipher.encryptVersionstamp(versionstamp, entry.getKey().uuid(), presenceEntry.getKey())),
                        presenceEntry.getValue());
                  }));
            })));
  }

  /// Insert a message bundle for a single recipient in an ongoing transaction.
  ///
  /// @implNote A message for a device is not inserted if it is offline and the message is ephemeral. Additionally, the
  /// message watch key is updated iff at least one receiving device is present.
  ///
  /// @param aci                accountId of the recipient
  /// @param messagesByDeviceId map of destination deviceId => message envelopes
  /// @param transaction        the ongoing transaction
  /// @return a future that yields the presence state of each destination device
  private CompletableFuture<Map<Byte, Boolean>> insert(final AciServiceIdentifier aci,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId,
      final int epoch,
      final int shardId,
      final Transaction transaction) {

    final Map<Byte, CompletableFuture<Boolean>> messageInsertFuturesByDeviceId = messagesByDeviceId.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
          final byte deviceId = e.getKey();
          final MessageProtos.Envelope message = e.getValue();
          final byte[] presenceKey = getPresenceKey(aci, deviceId);

          return transaction.get(presenceKey)
              .thenApply(this::isClientPresent)
              .thenApply(isPresent -> {
                if (isPresent || !message.getEphemeral()) {
                  transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY,
                      getDeviceQueueSubspace(aci, deviceId)
                          .packWithVersionstamp(Tuple.from(Versionstamp.incomplete(packUserData(epoch, shardId)))), message.toByteArray());
                }

                return isPresent;
              });
        }));

    return CompletableFuture.allOf(messageInsertFuturesByDeviceId.values().toArray(CompletableFuture[]::new))
        .thenApply(_ -> {
          final Map<Byte, Boolean> presenceByDeviceId = messageInsertFuturesByDeviceId.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                assert entry.getValue().isDone();
                return entry.getValue().join();
              }));

          final boolean anyClientPresent = presenceByDeviceId.values().stream().anyMatch(present -> present);

          if (anyClientPresent) {
            transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getMessagesAvailableWatchKey(aci),
                Tuple.from(Versionstamp.incomplete(packUserData(epoch, shardId))).packWithVersionstamp());
          }

          return presenceByDeviceId;
        });
  }

  // Note that this method is intended only for initial migration support; in general, callers should clear messages
  // by acknowledging messages via a `FoundationDbMessageStream`.
  public CompletableFuture<Void> delete(final AciServiceIdentifier aci, final byte deviceId, final UUID messageGuid) {
    return delete(aci, deviceId, versionstampUUIDCipher.decryptVersionstamp(messageGuid, aci.uuid(), deviceId));
  }

  private CompletableFuture<Void> delete(final AciServiceIdentifier aci, final byte deviceId, final Versionstamp versionstamp) {
    final Timer.Sample sample = Timer.start();

    final byte[] messageKey = getDeviceQueueSubspace(aci, deviceId).pack(Tuple.from(versionstamp));

    return databasesByEpoch[getConfigurationEpoch(versionstamp)][getShardId(versionstamp)].runAsync(transaction -> {
          transaction.clear(messageKey);
          return CompletableFuture.completedFuture(null);
        })
        .thenRun(() -> {
          sample.stop(DELETE_MESSAGE_TIMER);
          DELETE_MESSAGE_COUNTER.increment();
        });
  }

  public void clearAll(final AciServiceIdentifier aci) {
    doForAllDatabasesWithMessages(aci, database -> database.run(transaction -> {
      transaction.clear(getAccountSubspace(aci).range());
      return null;
    }));
  }

  public void clearAll(final AciServiceIdentifier aci, final byte deviceId) {
    doForAllDatabasesWithMessages(aci, database -> database.run(transaction -> {
      transaction.clear(getDeviceSubspace(aci, deviceId).range());
      return null;
    }));
  }

  private void doForAllDatabasesWithMessages(final AciServiceIdentifier aci, final Consumer<Database> action) {
    IntStream.range(0, databasesByEpoch.length)
        .filter(epoch -> databasesByEpoch[epoch] != null)
        .mapToObj(epoch -> databasesByEpoch[epoch][hashAciToShardNumber(aci, epoch)])
        .distinct()
        .forEach(action);
  }

  public FoundationDbMessageStream getMessages(final AciServiceIdentifier aci, final byte deviceId) {
    return getMessages(aci, deviceId, FoundationDbMessageStream.DEFAULT_MAX_MESSAGES_PER_SCAN,
        FoundationDbMessageStream.DEFAULT_MAX_UNACKNOWLEDGED_MESSAGES, Util.NOOP);
  }

  @VisibleForTesting
  FoundationDbMessageStream getMessages(final AciServiceIdentifier aci,
      final byte deviceId,
      final int maxMessagesPerScan,
      final int maxUnacknowledgedMessages,
      final Runnable doAfterCleanup) {

    // For each configured database epoch, which database held (or holds) the messages for this ACI/device pair?
    final Database[] databasesForQueueByEpoch = new Database[databasesByEpoch.length];

    for (final int epoch : liveEpochs) {
      databasesForQueueByEpoch[epoch] = getShardForAci(aci, epoch);
    }

    return new FoundationDbMessageStream(getDeviceQueueSubspace(aci, deviceId),
        getPresenceKey(aci, deviceId),
        getMessagesAvailableWatchKey(aci),
        databasesForQueueByEpoch,
        new MessageGuidCodec(aci.uuid(), deviceId, versionstampUUIDCipher),
        maxMessagesPerScan,
        maxUnacknowledgedMessages,
        doAfterCleanup,
        presenceRenewalExecutorService,
        clock);
  }

  /// Record the versionstamp for the current time in each database's versionstamp clock.
  public void recordVersionstamps() {
    for (VersionstampClock versionstampClock : versionstampClocks.values()) {
      versionstampClock.recordVersionstampAndTime();
    }
  }

  /// Delete messages for the given devices that were inserted before the given time.
  ///
  /// This issues one range delete for every account/device pair in the map for each actual underlying FoundationDB
  /// cluster that ever hosted that account in a configured epoch (not just the current epoch). While range deletes are
  /// efficient, they can potentially induce significant load on the storage process, so callers should be judicious
  /// with flow control when calling this method.
  ///
  /// @param accountDeviceIdentifiers a map from ACI to the list of device IDs for which expired messages should be
  /// trimmed. Note that, depending on the sharding schema of the configured message store, it is possible that we will
  /// issue a range clear for every device in a single transaction. One range clear involves two keys, each of which has
  /// an ACI, device ID, and versionstamp, along with overhead to identify the relevant subspace; given the 10MB
  /// transaction limit and a healthy margin for safety, this means there should be well under 100,000 total
  /// account/device pairs supplied in a single call to this method.
  ///
  /// @param cutoffTime the expiration threshold. Messages inserted before this time may be deleted; messages inserted
  /// after it will not be. Deletion depends on the underlying [versionstamp clocks][VersionstampClock] being kept up to
  /// date.
  @VisibleForTesting
  public void deleteMessagesBefore(final Map<AciServiceIdentifier, List<Byte>> accountDeviceIdentifiers, final Instant cutoffTime) {
    final Map<Database, List<Subspace>> queueSubspacesToTrimByDatabase = new IdentityHashMap<>();

    accountDeviceIdentifiers.forEach((aci, deviceIds) -> {
      for (final byte deviceId : deviceIds) {
        final Subspace queueSubspace = getDeviceQueueSubspace(aci, deviceId);
        IntStream.of(liveEpochs)
            .mapToObj(e -> getShardForAci(aci, e))
            .distinct()
            .forEach(database -> queueSubspacesToTrimByDatabase.computeIfAbsent(database, _ -> new ArrayList<>()).add(queueSubspace));
      }
    });

    queueSubspacesToTrimByDatabase.forEach((database, queueSubspaces) -> {
      // It's OK that this puts reading the versionstamp in a separate transaction from the deletes; versionstamp clock
      // entries are effectively immutable and we're looking for one that was written presumably very far in the past,
      // so there's no conflict to avoid
      versionstampClocks.get(database).getVersionstamp(cutoffTime).ifPresent(cutoffVersionstamp -> {
        database.run(transaction -> {
          transaction.options().setPriorityBatch();
          for (final Subspace queueSubspace : queueSubspaces) {
            transaction.clear(new Range(queueSubspace.getKey(), queueSubspace.pack(Tuple.from(cutoffVersionstamp))));
          }
          return null;
        });
      });
    });
  }

  static Versionstamp getVersionstamp(final byte[] messageKey) {
    return Tuple.fromBytes(messageKey).getVersionstamp(4);
  }

  @VisibleForTesting
  Database getShardForAci(final AciServiceIdentifier aci, final int epoch) {
    return getDatabases(epoch)[hashAciToShardNumber(aci, epoch)];
  }

  private Database[] getDatabases(final int epoch) {
    if (databasesByEpoch[epoch] == null) {
      throw new IllegalStateException("Epoch (%d) not in static configuration".formatted(epoch));
    }

    return databasesByEpoch[epoch];
  }

  @VisibleForTesting
  int hashAciToShardNumber(final AciServiceIdentifier aci, final int epoch) {
    // We use a consistent hash here to reduce the number of key remappings if we increase the number of shards
    return Hashing.consistentHash(aci.uuid().getLeastSignificantBits(), getDatabases(epoch).length);
  }

  @VisibleForTesting
  static int packUserData(final int epoch, final int shardId) {
    if (epoch < 0 || epoch >= MAX_EPOCHS) {
      throw new IllegalArgumentException("Epoch (%d) outside of allowable range (0 to %d, exclusive)".formatted(
          epoch, MAX_EPOCHS));
    }

    if (shardId < 0 || shardId >= MAX_SHARDS) {
      throw new IllegalArgumentException("Shard ID (%d) outside of allowable range (0 to %d, exclusive)".formatted(
          epoch, MAX_SHARDS));
    }

    return epoch << 6 | shardId;
  }

  static int getConfigurationEpoch(final Versionstamp versionstamp) {
    return versionstamp.getUserVersion() >> 6 & 0x03;
  }

  @VisibleForTesting
  static int getShardId(final Versionstamp versionstamp) {
    return versionstamp.getUserVersion() & 0x3f;
  }

  @VisibleForTesting
  static Subspace getDeviceQueueSubspace(final AciServiceIdentifier aci, final byte deviceId) {
    return getDeviceSubspace(aci, deviceId).get("Q");
  }

  private static Subspace getDeviceSubspace(final AciServiceIdentifier aci, final byte deviceId) {
    return getAccountSubspace(aci).get(deviceId);
  }

  private static Subspace getAccountSubspace(final AciServiceIdentifier aci) {
    return MESSAGES_SUBSPACE.get(aci.uuid());
  }

  @VisibleForTesting
  static byte[] getMessagesAvailableWatchKey(final AciServiceIdentifier aci) {
    return getAccountSubspace(aci).pack("l");
  }

  @VisibleForTesting
  static byte[] getServerId() {
    return SERVER_ID;
  }

  @VisibleForTesting
  static byte[] getPresenceKey(final AciServiceIdentifier aci, final byte deviceId) {
    return getDeviceSubspace(aci, deviceId).pack("p");
  }

  static byte[] getPresenceValue(final Instant timestamp, final int streamId) {
    return ByteBuffer.allocate(PRESENCE_VALUE_LENGTH)
        .put(SERVER_ID)
        .putInt(streamId)
        .putLong(timestamp.toEpochMilli())
        .array();
  }

  @VisibleForTesting
  static byte[] getPresenceServerId(final byte[] presenceValue) {
    if (presenceValue.length != PRESENCE_VALUE_LENGTH) {
      throw new IllegalArgumentException("Unexpected presence value length: " + presenceValue.length);
    }

    return Arrays.copyOfRange(presenceValue, 0, 16);
  }

  @VisibleForTesting
  static int getPresenceStreamId(final byte[] presenceValue) {
    if (presenceValue.length != PRESENCE_VALUE_LENGTH) {
      throw new IllegalArgumentException("Unexpected presence value length: " + presenceValue.length);
    }

    return ByteBuffer.wrap(presenceValue).getInt(16);
  }

  @VisibleForTesting
  static Instant getPresenceTimestamp(final byte[] presenceValue) {
    if (presenceValue.length != PRESENCE_VALUE_LENGTH) {
      throw new IllegalArgumentException("Unexpected presence value length: " + presenceValue.length);
    }

    return Instant.ofEpochMilli(ByteBuffer.wrap(presenceValue).getLong(20));
  }

  @VisibleForTesting
  boolean isClientPresent(@Nullable final byte[] presenceValue) {
    if (presenceValue == null) {
      return false;
    }

    return Duration.between(getPresenceTimestamp(presenceValue), clock.instant()).compareTo(PRESENCE_STALE_THRESHOLD) <= 0;
  }
}
