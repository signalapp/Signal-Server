/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;

public class BackupUsageRecalculationCommand extends AbstractCommandWithDependencies {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String SEGMENT_COUNT_ARGUMENT = "segments";
  private static final int DEFAULT_SEGMENT_COUNT = 1;

  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";
  private static final int DEFAULT_MAX_CONCURRENCY = 4;

  private static final String RECALCULATION_COUNT_COUNTER_NAME =
      MetricsUtil.name(BackupUsageRecalculationCommand.class, "countRecalculations");
  private static final String RECALCULATION_BYTE_COUNTER_NAME =
      MetricsUtil.name(BackupUsageRecalculationCommand.class, "byteRecalculations");


  public BackupUsageRecalculationCommand() {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "backup-usage-recalculation", "Recalculate the usage of backups");
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
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {

    final int segments = Objects.requireNonNull(namespace.getInt(SEGMENT_COUNT_ARGUMENT));
    final int recalculationConcurrency = Objects.requireNonNull(namespace.getInt(MAX_CONCURRENCY_ARGUMENT));
    logger.info("Crawling to recalculate usage with {} segments and {} processors",
        segments,
        Runtime.getRuntime().availableProcessors());

    final BackupManager backupManager = commandDependencies.backupManager();
    final Long backupsConsidered = backupManager
        .listBackupAttributes(segments, Schedulers.parallel())
        .flatMap(attrs -> Mono.fromCompletionStage(() -> backupManager.recalculateQuota(attrs)).doOnNext(maybeRecalculationResult -> maybeRecalculationResult.ifPresent(recalculationResult -> {
              if (!recalculationResult.newUsage().equals(recalculationResult.oldUsage())) {
                logger.info("Recalculated usage. oldUsage={}, newUsage={}, lastRefresh={}, lastMediaRefresh={}",
                    recalculationResult.oldUsage(),
                    recalculationResult.newUsage(),
                    attrs.lastRefresh(),
                    attrs.lastMediaRefresh());
              }

              Metrics.counter(RECALCULATION_COUNT_COUNTER_NAME,
                      "delta", DeltaType.deltaType(
                          recalculationResult.oldUsage().numObjects(),
                          recalculationResult.newUsage().numObjects()).name())
                  .increment();

              Metrics.counter(RECALCULATION_BYTE_COUNTER_NAME,
                      "delta", DeltaType.deltaType(
                          recalculationResult.oldUsage().bytesUsed(),
                          recalculationResult.newUsage().bytesUsed()).name())
                  .increment();

            }
        )), recalculationConcurrency)
        .count()
        .block();
    logger.info("Crawled {} backups", backupsConsidered);
  }

  private enum DeltaType {
    REDUCED,
    SAME,
    INCREASED;

    static DeltaType deltaType(long oldv, long newv) {
      return switch (Long.signum(newv - oldv)) {
        case -1 -> REDUCED;
        case 0 -> SAME;
        case 1 -> INCREASED;
        default -> throw new IllegalStateException("Unexpected value: " + (newv - oldv));
      };
    }
  }
}
