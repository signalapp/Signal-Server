package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Pair;

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
  private static final int MAX_SECONDS_SINCE_UPDATE_FOR_PRESENCE = 300;

  public FoundationDbMessageStore(final Database[] databases, final Executor executor, final Clock clock) {
    this.databases = databases;
    this.executor = executor;
    this.clock = clock;
  }

  /// Insert a message bundle for a set of devices belonging to a single recipient. A message may not be inserted if the
  /// device is not present (as determined from its presence key) and the message is ephemeral. If all messages in the
  /// bundle don't end up being inserted, we won't return a versionstamp since the transaction was read-only.
  ///
  /// @param aci                destination account identifier
  /// @param messagesByDeviceId a map of deviceId => message envelope
  /// @return a future that completes with a [Versionstamp] of the committed transaction if at least one message was
  ///  inserted
  public CompletableFuture<Optional<Versionstamp>> insert(final AciServiceIdentifier aci,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId) {
    // We use Database#runAsync and not Database#run here because the latter would commit the transaction synchronously
    // and we would like to avoid any potential blocking in native code that could unexpectedly pin virtual threads. See https://forums.foundationdb.org/t/fdbdatabase-usage-from-java-api/593/2
    // for details.
    return getShardForAci(aci).runAsync(transaction -> insert(aci, messagesByDeviceId, transaction)
            .thenApply(hasMutations -> {
              if (hasMutations) {
                return transaction.getVersionstamp();
              }
              return CompletableFuture.completedFuture((byte[]) null);
            }))
        .thenComposeAsync(Function.identity(), executor)
        .thenApply(versionstampBytes -> Optional.ofNullable(versionstampBytes).map(Versionstamp::complete));
  }

  private CompletableFuture<Boolean> insert(final AciServiceIdentifier aci,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId,
      final Transaction transaction) {
    final List<CompletableFuture<Pair<Boolean, Boolean>>> messageInsertFutures = messagesByDeviceId.entrySet()
        .stream()
        .map(e -> {
          final byte deviceId = e.getKey();
          final MessageProtos.Envelope message = e.getValue();
          final byte[] presenceKey = getPresenceKey(aci, deviceId);
          return transaction.get(presenceKey)
              .thenApply(this::isClientPresent)
              .thenApply(isPresent -> {
                boolean hasMutations = false;
                if (isPresent || !message.getEphemeral()) {
                  final Subspace deviceQueueSubspace = getDeviceQueueSubspace(aci, deviceId);
                  transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY,
                      deviceQueueSubspace.packWithVersionstamp(Tuple.from(
                          Versionstamp.incomplete())), message.toByteArray());
                  hasMutations = true;
                }
                return new Pair<>(isPresent, hasMutations);
              });
        })
        .toList();
    return CompletableFuture.allOf(messageInsertFutures.toArray(CompletableFuture[]::new))
        .thenApply(_ -> {
          final boolean anyClientPresent = messageInsertFutures
              .stream()
              .anyMatch(future -> future.join().first());
          final boolean hasMutations = messageInsertFutures
              .stream()
              .anyMatch(future -> future.join().second());
          if (anyClientPresent && hasMutations) {
            transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getMessagesAvailableWatchKey(aci),
                Tuple.from(Versionstamp.incomplete()).packWithVersionstamp());
          }
          return hasMutations;
        });
  }

  private Database getShardForAci(final AciServiceIdentifier aci) {
    return databases[hashAciToShardNumber(aci)];
  }

  private int hashAciToShardNumber(final AciServiceIdentifier aci) {
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
    return getDeviceQueueSubspace(aci, deviceId).pack("p");
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
    return (clock.instant().getEpochSecond() - lastSeenSecondsSinceEpoch) <= MAX_SECONDS_SINCE_UPDATE_FOR_PRESENCE;
  }

}
