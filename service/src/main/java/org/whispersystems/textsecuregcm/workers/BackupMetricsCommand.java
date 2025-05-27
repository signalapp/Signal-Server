/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class BackupMetricsCommand extends AbstractCommandWithDependencies {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String SEGMENT_COUNT_ARGUMENT = "segments";
  private static final int DEFAULT_SEGMENT_COUNT = 1;

  private final Clock clock;

  public BackupMetricsCommand(final Clock clock) {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "backup-metrics", "Reports metrics about backups");
    this.clock = clock;
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
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {

    final int segments = Objects.requireNonNull(namespace.getInt(SEGMENT_COUNT_ARGUMENT));
    logger.info("Crawling backups for metrics with {} segments and {} processors",
        segments,
        Runtime.getRuntime().availableProcessors());

    final DistributionSummary timeSinceLastRefresh = Metrics.summary(name(getClass(),
        "timeSinceLastRefresh"));
    final DistributionSummary timeSinceLastMediaRefresh = Metrics.summary(name(getClass(),
        "timeSinceLastMediaRefresh"));

    final BackupManager backupManager = commandDependencies.backupManager();
    final Long backupsExpired = backupManager
        .listBackupAttributes(segments, Schedulers.parallel())
        .doOnNext(backupMetadata -> {
          timeSinceLastRefresh.record(timeSince(backupMetadata.lastRefresh()).getSeconds());
          timeSinceLastMediaRefresh.record(timeSince(backupMetadata.lastMediaRefresh()).getSeconds());
        })
        .count()
        .block();
    logger.info("Crawled {} backups", backupsExpired);
  }

  private Duration timeSince(Instant t) {
    final Duration between = Duration.between(t, clock.instant());
    if (between.isNegative()) {
      return Duration.ZERO;
    }
    return between;
  }
}
