/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.IssuedReceiptsManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class IssuedReceiptMigrationCommand extends AbstractCommandWithDependencies {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String SEGMENT_COUNT_ARGUMENT = "segments";
  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";
  private static final String BUFFER_ARGUMENT = "buffer";

  private static final String INSPECTED_ISSUED_RECEIPTS = MetricsUtil.name(IssuedReceiptMigrationCommand.class,
      "inspectedIssuedReceipts");
  private static final String MIGRATED_ISSUED_RECEIPTS = MetricsUtil.name(IssuedReceiptMigrationCommand.class,
      "migratedIssuedReceipts");

  private final Clock clock;

  public IssuedReceiptMigrationCommand(final Clock clock) {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "migrate-issued-receipts", "Migrates columns in the issued receipts table");
    this.clock = clock;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--segments")
        .type(Integer.class)
        .dest(SEGMENT_COUNT_ARGUMENT)
        .required(false)
        .setDefault(1)
        .help("The total number of segments for a DynamoDB scan");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(16)
        .help("Max concurrency for migration operations");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually perform migration");

    subparser.addArgument("--buffer")
        .type(Integer.class)
        .dest(BUFFER_ARGUMENT)
        .setDefault(16_384)
        .help("Records to buffer");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
                     final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {
    final int bufferSize = namespace.getInt(BUFFER_ARGUMENT);
    final int segments = Objects.requireNonNull(namespace.getInt(SEGMENT_COUNT_ARGUMENT));
    final int concurrency = Objects.requireNonNull(namespace.getInt(MAX_CONCURRENCY_ARGUMENT));
    final boolean dryRun = namespace.getBoolean(DRY_RUN_ARGUMENT);

    logger.info("Crawling issuedReceipts with {} segments and {} processors",
        segments,
        Runtime.getRuntime().availableProcessors());

    final Counter inspected = Metrics.counter(INSPECTED_ISSUED_RECEIPTS,
        "dryRun", Boolean.toString(dryRun));
    final Counter migrated = Metrics.counter(MIGRATED_ISSUED_RECEIPTS,
        "dryRun", Boolean.toString(dryRun));

    final IssuedReceiptsManager issuedReceiptsManager = commandDependencies.issuedReceiptsManager();
    final Flux<IssuedReceiptsManager.IssuedReceipt> receipts =
        issuedReceiptsManager.receiptsWithoutTagSet(segments, Schedulers.parallel());
    final long count = bufferShuffle(receipts, bufferSize)
        .doOnNext(issuedReceipt -> inspected.increment())
        .flatMap(issuedReceipt -> Mono
                .fromCompletionStage(() -> dryRun
                    ? CompletableFuture.completedFuture(null)
                    : issuedReceiptsManager.migrateToTagSet(issuedReceipt))
                .thenReturn(true)
                .retry(3)
                .onErrorResume(throwable -> {
                  logger.error("Failed to migrate {} after 3 attempts, giving up", issuedReceipt.itemId(), throwable);
                  return Mono.just(false);
                }),
            concurrency)
        .doOnNext(success ->
            Metrics.counter(MIGRATED_ISSUED_RECEIPTS,
                "dryRun", Boolean.toString(dryRun),
                "success", Boolean.toString(success)))
        .count()
        .block();
    logger.info("Attempted to migrate {} issued receipts", count);
  }

  private static <T> Flux<T> bufferShuffle(Flux<T> f, int bufferSize) {
    return f.buffer(bufferSize)
        .map(source -> {
          final ArrayList<T> shuffled = new ArrayList<>(source);
          Collections.shuffle(shuffled);
          return shuffled;
        })
        .limitRate(2)
        .flatMapIterable(Function.identity());
  }
}
