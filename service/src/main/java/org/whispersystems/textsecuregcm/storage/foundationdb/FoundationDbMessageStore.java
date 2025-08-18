package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import io.dropwizard.util.DataSize;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.Conversions;

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

  private final Database[] databases;
  private final Executor executor;
  private final Clock clock;

  private static final Subspace MESSAGES_SUBSPACE = new Subspace(Tuple.from("M"));
  private static final Duration PRESENCE_STALE_THRESHOLD = Duration.ofMinutes(5);

  /// The (approximate) transaction size beyond which we do not add more messages in a transaction. The estimated size
  /// includes only message payloads (and not key reads/writes) which we assume will dominate the total
  /// transaction size. Note that the FDB [docs](https://apple.github.io/foundationdb/known-limitations.html) currently
  /// suggest a limit of 1MB to avoid performance issues, although the hard limit is 10MB
  private static final long MAX_MESSAGE_CHUNK_SIZE = DataSize.megabytes(1).toBytes();

  /// Result of inserting a message for a particular device
  ///
  /// @param versionstamp the versionstamp of the transaction in which this device's message was inserted, empty
  ///                     otherwise
  /// @param present      whether the device is online
  public record InsertResult(Optional<Versionstamp> versionstamp, boolean present) {
  }

  public FoundationDbMessageStore(final Database[] databases, final Executor executor, final Clock clock) {
    this.databases = databases;
    this.executor = executor;
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

    return insert(Map.of(aciServiceIdentifier, messagesByDeviceId))
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

    if (messagesByServiceIdentifier.entrySet()
        .stream()
        .anyMatch(entry -> entry.getValue().isEmpty())) {
      throw new IllegalArgumentException("One or more message bundles is empty");
    }

    final Map<Integer, List<Map.Entry<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>>>> messagesByShardId =
        messagesByServiceIdentifier.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> hashAciToShardNumber(entry.getKey())));

    final List<CompletableFuture<Map<AciServiceIdentifier, Map<Byte, InsertResult>>>> chunkFutures =
        new ArrayList<>();

    messagesByShardId.forEach((shardId, messagesForShard) -> {
      final Database shard = databases[shardId];

      int start = 0, current = 0;
      int estimatedTransactionSize = 0;

      while (current < messagesForShard.size()) {
        estimatedTransactionSize += messagesForShard.get(current).getValue().values()
            .stream()
            .mapToInt(MessageProtos.Envelope::getSerializedSize)
            .sum();

        if (estimatedTransactionSize > MAX_MESSAGE_CHUNK_SIZE) {
          chunkFutures.add(insertChunk(shard, messagesForShard.subList(start, current)));

          start = current;
          estimatedTransactionSize = 0;
        } else {
          current++;
        }
      }

      assert start < messagesForShard.size();
      chunkFutures.add(insertChunk(shard, messagesForShard.subList(start, messagesForShard.size())));
    });

    return CompletableFuture.allOf(chunkFutures.toArray(CompletableFuture[]::new))
        .thenApply(_ -> chunkFutures.stream()
            .map(CompletableFuture::join)
            .reduce(new HashMap<>(), (a, b) -> {
              a.putAll(b);
              return a;
            }));
  }

  private CompletableFuture<Map<AciServiceIdentifier, Map<Byte, InsertResult>>> insertChunk(
      final Database database,
      final List<Map.Entry<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>>> messagesByAccountIdentifier) {

    final Map<AciServiceIdentifier, CompletableFuture<Map<Byte, Boolean>>> insertFuturesByAci = new HashMap<>();

    // In a message bundle (single-recipient or MRM) the ephemerality should be the same for all envelopes, so just get the first.
    final boolean ephemeral = messagesByAccountIdentifier.stream()
        .findFirst()
        .flatMap(entry -> entry.getValue().values().stream().findFirst())
        .map(MessageProtos.Envelope::getEphemeral)
        .orElseThrow(() -> new IllegalStateException("One or more bundles is empty"));

    return database.runAsync(transaction -> {
          messagesByAccountIdentifier.forEach(entry ->
              insertFuturesByAci.put(entry.getKey(), insert(entry.getKey(), entry.getValue(), transaction)));

          return CompletableFuture.allOf(insertFuturesByAci.values().toArray(CompletableFuture[]::new))
              .thenApply(_ -> {
                final boolean anyClientPresent = insertFuturesByAci.values()
                    .stream()
                    .map(CompletableFuture::join)
                    .flatMap(presenceByDeviceId -> presenceByDeviceId.values().stream())
                    .anyMatch(isPresent -> isPresent);
                if (anyClientPresent || !ephemeral) {
                  return transaction.getVersionstamp()
                      .thenApply(versionstampBytes -> Optional.of(Versionstamp.complete(versionstampBytes)));
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
                    return new InsertResult(insertResultVersionstamp, presenceEntry.getValue());
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
                          .packWithVersionstamp(Tuple.from(Versionstamp.incomplete())), message.toByteArray());
                }

                return isPresent;
              });
        }));

    return CompletableFuture.allOf(messageInsertFuturesByDeviceId.values().toArray(CompletableFuture[]::new))
        .thenApplyAsync(_ -> {
          final Map<Byte, Boolean> presenceByDeviceId = messageInsertFuturesByDeviceId.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                assert entry.getValue().isDone();
                return entry.getValue().join();
              }));

          final boolean anyClientPresent = presenceByDeviceId.values().stream().anyMatch(present -> present);

          if (anyClientPresent) {
            transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getMessagesAvailableWatchKey(aci),
                Tuple.from(Versionstamp.incomplete()).packWithVersionstamp());
          }

          return presenceByDeviceId;
        }, executor);
  }

  @VisibleForTesting
  Database getShardForAci(final AciServiceIdentifier aci) {
    return databases[hashAciToShardNumber(aci)];
  }

  @VisibleForTesting
  int hashAciToShardNumber(final AciServiceIdentifier aci) {
    // We use a consistent hash here to reduce the number of key remappings if we increase the number of shards
    return Hashing.consistentHash(aci.uuid().getLeastSignificantBits(), databases.length);
  }

  @VisibleForTesting
  Subspace getDeviceQueueSubspace(final AciServiceIdentifier aci, final byte deviceId) {
    return getDeviceSubspace(aci, deviceId).get("Q");
  }

  private Subspace getDeviceSubspace(final AciServiceIdentifier aci, final byte deviceId) {
    return getAccountSubspace(aci).get(deviceId);
  }

  private Subspace getAccountSubspace(final AciServiceIdentifier aci) {
    return MESSAGES_SUBSPACE.get(aci.uuid());
  }

  @VisibleForTesting
  byte[] getMessagesAvailableWatchKey(final AciServiceIdentifier aci) {
    return getAccountSubspace(aci).pack("l");
  }

  @VisibleForTesting
  byte[] getPresenceKey(final AciServiceIdentifier aci, final byte deviceId) {
    return getDeviceSubspace(aci, deviceId).pack("p");
  }

  @VisibleForTesting
  boolean isClientPresent(final byte[] presenceValueBytes) {
    if (presenceValueBytes == null) {
      return false;
    }
    final long presenceValue = Conversions.byteArrayToLong(presenceValueBytes);
    // The presence value is a long with the higher order 16 bits containing a server id, and the lower 48 bits
    // containing the timestamp (seconds since epoch) that the client updates periodically.
    final long lastSeenSecondsSinceEpoch = presenceValue & 0x0000ffffffffffffL;
    return (clock.instant().getEpochSecond() - lastSeenSecondsSinceEpoch) <= PRESENCE_STALE_THRESHOLD.toSeconds();
  }
}
