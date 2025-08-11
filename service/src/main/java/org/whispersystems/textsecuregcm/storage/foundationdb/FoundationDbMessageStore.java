package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;

/// An implementation of a message store backed by FoundationDB.
///
/// @implNote The layout of elements in FoundationDB is as follows:
/// * messages
///   * {aci}
///     * last => versionstamp
///     * {deviceId}
///       * queue
///         * {versionstamp_1} => envelope_1
///         * {versionstamp_2} => envelope_2
public class FoundationDbMessageStore {

  private final Database[] databases;
  private static final Subspace MESSAGES_SUBSPACE = new Subspace(Tuple.from("M"));
  private final Executor executor;

  public FoundationDbMessageStore(final Database[] databases, final Executor executor) {
    this.databases = databases;
    this.executor = executor;
  }

  /**
   * Insert a message bundle for a set of devices belonging to a single recipient
   *
   * @param aci                destination account identifier
   * @param messagesByDeviceId a map of deviceId => message envelope
   * @return a future that completes with a {@link Versionstamp} of the committed transaction
   */
  public CompletableFuture<Versionstamp> insert(final AciServiceIdentifier aci,
      final Map<Byte, MessageProtos.Envelope> messagesByDeviceId) {
    // We use Database#runAsync and not Database#run here because the latter would commit the transaction synchronously
    // and we would like to avoid any potential blocking in native code that could unexpectedly pin virtual threads. See https://forums.foundationdb.org/t/fdbdatabase-usage-from-java-api/593/2
    // for details.
    return getShardForAci(aci).runAsync(transaction -> {
          insert(aci, messagesByDeviceId, transaction);
          return CompletableFuture.completedFuture(transaction.getVersionstamp());
        })
        .thenComposeAsync(Function.identity(), executor)
        .thenApply(Versionstamp::complete);
  }

  private void insert(final AciServiceIdentifier aci, final Map<Byte, MessageProtos.Envelope> messagesByDeviceId,
      final Transaction transaction) {
    messagesByDeviceId.forEach((deviceId, message) -> {
      final Subspace deviceQueueSubspace = getDeviceQueueSubspace(aci, deviceId);
      transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY, deviceQueueSubspace.packWithVersionstamp(Tuple.from(
          Versionstamp.incomplete())), message.toByteArray());
    });
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getLastMessageKey(aci),
        Tuple.from(Versionstamp.incomplete()).packWithVersionstamp());
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
  byte[] getLastMessageKey(final AciServiceIdentifier aci) {
    return getAccountSubspace(aci).pack("l");
  }

}
