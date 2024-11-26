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
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import java.time.Duration;

public class MigrateDeletedAccountsCommand extends AbstractCommandWithDependencies {

  private static final String RECORDS_INSPECTED_COUNTER_NAME =
      MetricsUtil.name(MigrateDeletedAccountsCommand.class, "recordsInspected");

  private static final String RECORDS_MIGRATED_COUNTER_NAME =
      MetricsUtil.name(MigrateDeletedAccountsCommand.class, "recordsMigrated");

  private static final String DRY_RUN_TAG = "dryRun";

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String SEGMENT_COUNT_ARGUMENT = "segments";
  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final int DEFAULT_SEGMENT_COUNT = 1;
  private static final int DEFAULT_CONCURRENCY = 16;

  public MigrateDeletedAccountsCommand() {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "migrate-deleted-accounts", "Migrates recently-deleted account records from E164 to PNI-keyed schema");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--segments")
        .type(Integer.class)
        .dest(SEGMENT_COUNT_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_SEGMENT_COUNT)
        .help("The total number of segments for a DynamoDB scan");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_CONCURRENCY)
        .help("Max concurrency for migrations.");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually migrate any deleted accounts records");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {
    final int segments = namespace.getInt(SEGMENT_COUNT_ARGUMENT);
    final int concurrency = namespace.getInt(MAX_CONCURRENCY_ARGUMENT);
    final boolean dryRun = namespace.getBoolean(DRY_RUN_ARGUMENT);

    logger.info("Crawling deleted accounts with {} segments and {} processors",
        segments,
        Runtime.getRuntime().availableProcessors());

    final Counter recordsInspectedCounter =
        Metrics.counter(RECORDS_INSPECTED_COUNTER_NAME, DRY_RUN_TAG, String.valueOf(dryRun));

    final Counter recordsMigratedCounter =
        Metrics.counter(RECORDS_MIGRATED_COUNTER_NAME, DRY_RUN_TAG, String.valueOf(dryRun));

    final AccountsManager accounts = commandDependencies.accountsManager();

    accounts.getE164KeyedDeletedAccounts(segments, Schedulers.parallel())
        .doOnNext(tuple -> recordsInspectedCounter.increment())
        .flatMap(
            tuple -> dryRun
                ? Mono.just(false)
                : Mono.fromFuture(
                        accounts.migrateDeletedAccount(
                            tuple.getT1(), tuple.getT2(), tuple.getT3()))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .onErrorResume(throwable -> {
                      logger.warn("Failed to migrate record for {}", tuple.getT1(), throwable);
                      return Mono.empty();
                    }),
            concurrency)
        .filter(migrated -> migrated)
        .doOnNext(ignored -> recordsMigratedCounter.increment())
        .then()
        .block();
  }
}
