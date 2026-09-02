/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore.getAccountSubspace;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.AccountLockManager;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import org.whispersystems.textsecuregcm.util.ManagedExecutors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class ClearOrphanedFoundationDbQueuesCommand extends AbstractCommandWithDependencies {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Subspace messagesSubspace;

  private static final String DRY_RUN_ARGUMENT = "dry-run";

  private static final String CONCURRENCY_ARGUMENT = "concurrency";
  private static final int DEFAULT_CONCURRENCY = 16;

  private static final String MAX_ACIS_PER_TRANSACTION_ARGUMENT = "max-acis-per-transaction";
  private static final int DEFAULT_MAX_ACIS_PER_TRANSACTION = 128;

  private static final String CHUNKS_PER_SHARD_ARGUMENT = "chunks-per-shard";
  private static final int DEFAULT_CHUNKS_PER_SHARD = 100;

  private static final String ACCOUNTS_CRAWLED_COUNTER = MetricsUtil.name(ClearOrphanedFoundationDbQueuesCommand.class,
      "accountsCrawled");
  private static final String ORPHANED_QUEUES_CLEARED = MetricsUtil.name(ClearOrphanedFoundationDbQueuesCommand.class,
      "orphanedQueuesCleared");


  public ClearOrphanedFoundationDbQueuesCommand() {
    this(FoundationDbMessageStore.MESSAGES_SUBSPACE);
  }

  @VisibleForTesting
  public ClearOrphanedFoundationDbQueuesCommand(final Subspace messagesSubspace) {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "clear-orphaned-foundationdb-queues", "Clear FoundationDB queues that have no active account");
    this.messagesSubspace = messagesSubspace;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--concurrency")
        .type(Integer.class)
        .dest(CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_CONCURRENCY)
        .help("The maximum number of parallel dynamodb operations to process concurrently");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually clear orphaned queues");

    subparser.addArgument("--max-acis-per-transaction")
        .type(Integer.class)
        .dest(MAX_ACIS_PER_TRANSACTION_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_MAX_ACIS_PER_TRANSACTION)
        .help("The maximum number of ACIs to read per FoundationDB transaction");

    subparser.addArgument("--chunks-per-shard")
        .type(Integer.class)
        .dest(CHUNKS_PER_SHARD_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_CHUNKS_PER_SHARD)
        .help("The number of chunks to split the the key-space into, each of which will be crawled in parallel");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {

    final int concurrency = Objects.requireNonNull(namespace.getInt(CONCURRENCY_ARGUMENT));
    final boolean dryRun = Objects.requireNonNull(namespace.getBoolean(DRY_RUN_ARGUMENT));
    final int maxAcisPerTransaction = Objects.requireNonNull(namespace.getInt(MAX_ACIS_PER_TRANSACTION_ARGUMENT));
    final int chunksPerShard = Objects.requireNonNull(namespace.getInt(CHUNKS_PER_SHARD_ARGUMENT));

    final FDB fdb = commandDependencies.fdb();

    final Stream<Database> databases = configuration.getFoundationDbMessagesConfiguration().clusters().values().stream()
        .map(databaseFactory -> {
          try {
            return databaseFactory.build(fdb);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        });
    final ExecutorService executorService = ManagedExecutors.newVirtualThreadPerTaskExecutor("clearOrphanedQueues",
        configuration.getVirtualThreadConfiguration().maxConcurrentThreadsPerExecutor(), environment);
    clearOrphanedQueues(databases, commandDependencies.accountsManager(), concurrency, dryRun, maxAcisPerTransaction,
        configuration.getFoundationDbMessagesConfiguration().batchPriorityTransactionRetryLimit(),
        configuration.getFoundationDbMessagesConfiguration().batchPriorityTransactionTimeout(),
        chunksPerShard,
        commandDependencies.accountLockManager(),
        executorService);
  }

  @VisibleForTesting
  void clearOrphanedQueues(final Stream<Database> databases, final AccountsManager accountsManager,
      final int concurrency, final boolean dryRun, final int maxAcisPerTransaction, final long transactionRetryLimit,
      final Duration transactionTimeout, final int numChunks, final AccountLockManager accountLockManager,
      final Executor executor) {
    Flux.fromStream(databases)
        .flatMap(database -> crawlAcisInShard(database, accountsManager, concurrency, dryRun, maxAcisPerTransaction,
            transactionRetryLimit, transactionTimeout, numChunks, accountLockManager, executor))
        .then()
        .block();
  }

  private Mono<Void> crawlAcisInShard(final Database database, final AccountsManager accountsManager,
      final int concurrency, final boolean dryRun, final int maxAcisPerTransaction, final long transactionRetryLimit,
      final Duration transactionTimeout, final int numChunks, final AccountLockManager accountLockManager,
      final Executor executor) {
    return getAcisInShard(database, maxAcisPerTransaction, transactionRetryLimit, transactionTimeout, numChunks)
        .doOnNext(_ -> Metrics.counter(ACCOUNTS_CRAWLED_COUNTER, "dryRun", String.valueOf(dryRun)).increment())
        .flatMap(aci -> Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(aci.uuid()))
                .flatMap(maybeAccount -> {
                  if (maybeAccount.isEmpty()) {
                    return Mono.just(aci);
                  }
                  return Mono.empty();
                })
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(1)))
                .onErrorResume(t -> {
                  logger.warn("Failed to fetch account by ACI", t);
                  return Mono.empty();
                })
            , concurrency)
        .flatMap(aci -> {
          if (dryRun) {
            logger.info("Would have cleared queue for ACI: {}", aci.uuid());
            return Mono.just(true);
          }
          return clearQueueWithAciLock(database, aci, transactionRetryLimit, transactionTimeout, accountsManager,
              accountLockManager, executor)
              .thenReturn(true)
              .onErrorResume(t -> {
                logger.error("Failed to clear orphaned queue for ACI: {}", aci.uuid(), t);
                return Mono.just(false);
              });
        })
        .doOnNext(success -> Metrics.counter(ORPHANED_QUEUES_CLEARED,
            "dryRun", String.valueOf(dryRun),
            "success", String.valueOf(success)).increment())
        .then();
  }

  private Mono<Void> clearQueueWithAciLock(final Database database, final AciServiceIdentifier aci,
      final long transactionRetryLimit, final Duration transactionTimeout, final AccountsManager accountsManager,
      final AccountLockManager accountLockManager,
      final Executor executor) {
    return Mono.fromFuture(
        () -> CompletableFuture.runAsync(() -> accountLockManager.withLock(Set.of(aci.uuid()), () -> {
          if (accountsManager.getByAccountIdentifier(aci.uuid()).isPresent()) {
            logger.info("ACI re-used after we checked for existence, not clearing its queues: {}", aci.uuid());
            return null;
          }
          clearQueue(database, aci, transactionRetryLimit, transactionTimeout);
          return null;
        }), executor));
  }

  private void clearQueue(final Database database, final AciServiceIdentifier aci, final long transactionRetryLimit,
      final Duration transactionTimeout) {
    database.run(transaction -> {
      transaction.options().setPriorityBatch();
      transaction.options().setRetryLimit(transactionRetryLimit);
      transaction.options().setTimeout(transactionTimeout.toMillis());
      transaction.clear(getAccountSubspace(messagesSubspace, aci).range());
      return null;
    });
  }

  @VisibleForTesting
  Flux<AciServiceIdentifier> getAcisInShard(final Database database, final int maxAcisPerTransaction,
      final long transactionRetryLimit, final Duration transactionTimeout, final int numChunks) {
    return Mono.fromFuture(() -> splitSubspace(database, numChunks))
        .flatMapIterable(Function.identity())
        .flatMap(
            range -> getAcisInChunk(database, maxAcisPerTransaction, transactionRetryLimit, transactionTimeout, range));
  }

  /// Split the messages subspace into roughly equally sized \`numChunks\` chunks, each of which can be crawled in
  /// parallel
  ///
  /// @param database  the FDB instance
  /// @param numChunks the number of chunks to split the subspace into
  /// @return a list of key [Range]s representing chunk boundaries
  CompletableFuture<List<Range>> splitSubspace(final Database database, final int numChunks) {
    return database.runAsync(transaction -> transaction.getEstimatedRangeSizeBytes(messagesSubspace.range())
            .thenCompose(rangeSize -> {
              final long chunkSize = Math.ceilDiv(rangeSize, numChunks);
              return transaction.getRangeSplitPoints(messagesSubspace.range(), chunkSize);
            }))
        .thenApply(result -> splitPointsToRanges(result.getKeys()));
  }

  private static List<Range> splitPointsToRanges(final List<byte[]> splitPoints) {
    if (splitPoints.size() < 2) {
      throw new IllegalArgumentException("Expected at least two split points");
    }
    final int numSplitPoints = splitPoints.size();
    final List<Range> ranges = new ArrayList<>();
    for (int i = 0; i < numSplitPoints - 1; i++) {
      ranges.add(new Range(splitPoints.get(i), splitPoints.get(i + 1)));
    }
    return ranges;
  }

  Flux<AciServiceIdentifier> getAcisInChunk(final Database database, final int maxAcisPerTransaction,
      final long transactionRetryLimit, final Duration transactionTimeout, final Range range) {
    return readAciBatch(database, range.begin, range.end, maxAcisPerTransaction, transactionRetryLimit,
        transactionTimeout)
        .expand(result -> {
          if (result.acis().isEmpty()) {
            return Mono.empty();
          }
          return readAciBatch(database, result.cursor(), range.end, maxAcisPerTransaction, transactionRetryLimit,
              transactionTimeout);
        })
        .limitRate(1)
        .flatMapIterable(BatchReadResult::acis);
  }

  private record BatchReadResult(List<AciServiceIdentifier> acis, byte[] cursor) {}

  private Mono<BatchReadResult> readAciBatch(
      final Database database,
      final byte[] beginInclusive,
      final byte[] endExclusive,
      final int maxAcisPerTransaction,
      final long transactionRetryLimit,
      final Duration transactionTimeout) {

    final AtomicReference<byte[]> cursor = new AtomicReference<>(beginInclusive);
    final AtomicInteger numAcisRead = new AtomicInteger();
    return Mono.fromFuture(() -> database.readAsync(transaction -> {
              final ReadTransaction readTransaction = transaction.snapshot();
              readTransaction.options().setPriorityBatch();
              readTransaction.options().setRetryLimit(transactionRetryLimit);
              readTransaction.options().setTimeout(transactionTimeout.toMillis());
              final List<AciServiceIdentifier> acis = new ArrayList<>();
              return AsyncUtil.whileTrue(() -> {
                    if (ByteArrayUtil.compareUnsigned(cursor.get(), endExclusive) >= 0) {
                      return CompletableFuture.completedFuture(false);
                    }
                    return readTransaction.getKey(KeySelector.firstGreaterOrEqual(cursor.get())).thenApply(key -> {
                      if (ByteArrayUtil.compareUnsigned(key, endExclusive) >= 0) {
                        return false;
                      }

                      final AciServiceIdentifier aci = getAciFromKey(key);
                      acis.add(aci);
                      // Skip the specified ACI's entire subspace by incrementing the account prefix; this way,
                      // [KeySelector#firstGreaterOrEqual] should resolve to the next available ACI (if one exists)
                      cursor.set(ByteArrayUtil.strinc(getAccountSubspace(messagesSubspace, aci).getKey()));
                      return numAcisRead.incrementAndGet() < maxAcisPerTransaction;
                    });
                  })
                  .thenApply(_ -> acis);
            })
            .thenApply(acis -> new BatchReadResult(acis, cursor.get()))
    );
  }

  public AciServiceIdentifier getAciFromKey(final byte[] key) {
    return new AciServiceIdentifier(messagesSubspace.unpack(key).getUUID(0));
  }

}
