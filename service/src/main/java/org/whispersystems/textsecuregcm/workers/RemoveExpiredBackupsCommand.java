/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.ExpiredBackup;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

public class RemoveExpiredBackupsCommand extends AbstractCommandWithDependencies {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String SEGMENT_COUNT_ARGUMENT = "segments";
  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";
  private static final String GRACE_PERIOD_ARGUMENT = "grace-period";

  // A backup that has not been refreshed after a grace period is eligible for deletion
  private static final Duration DEFAULT_GRACE_PERIOD = RemoveExpiredAccountsCommand.MAX_IDLE_DURATION;
  private static final int DEFAULT_SEGMENT_COUNT = 1;
  private static final int DEFAULT_CONCURRENCY = 16;

  private static final String EXPIRED_BACKUPS_COUNTER_NAME = MetricsUtil.name(RemoveExpiredBackupsCommand.class,
      "expiredBackups");

  private final Clock clock;

  public RemoveExpiredBackupsCommand(final Clock clock) {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "remove-expired-backups", "Removes backups that have expired");
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

    subparser.addArgument("--grace-period")
        .type(Long.class)
        .dest(GRACE_PERIOD_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_GRACE_PERIOD.getSeconds())
        .help("The number of seconds after which a backup is eligible for removal");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_CONCURRENCY)
        .help("Max concurrency for backup expirations. Each expiration may do multiple cdn operations");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually remove expired backups");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {
    final int segments = Objects.requireNonNull(namespace.getInt(SEGMENT_COUNT_ARGUMENT));
    final int concurrency = Objects.requireNonNull(namespace.getInt(MAX_CONCURRENCY_ARGUMENT));
    final boolean dryRun = namespace.getBoolean(DRY_RUN_ARGUMENT);
    final Duration gracePeriod = Duration.ofSeconds(Objects.requireNonNull(namespace.getLong(GRACE_PERIOD_ARGUMENT)));

    logger.info("Crawling backups with {} segments and {} processors, grace period {}",
        segments,
        Runtime.getRuntime().availableProcessors(),
        gracePeriod);

    final BackupManager backupManager = commandDependencies.backupManager();
    final long backupsExpired = backupManager
        .getExpiredBackups(segments, Schedulers.parallel(), clock.instant().minus(gracePeriod))
        .flatMap(expiredBackup -> removeExpiredBackup(backupManager, expiredBackup, dryRun), concurrency)
        .filter(Boolean.TRUE::equals)
        .count()
        .block();
    logger.info("Expired {} backups", backupsExpired);
  }

  private Mono<Boolean> removeExpiredBackup(
      final BackupManager backupManager, final ExpiredBackup expiredBackup,
      final boolean dryRun) {

    final Mono<Boolean> mono;
    if (dryRun) {
      mono = Mono.just(true);
    } else {
      mono = Mono.fromCompletionStage(() -> backupManager.expireBackup(expiredBackup)).thenReturn(true);
    }

    return mono
        .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
        .doOnSuccess(ignored -> {
          logger.trace("Successfully expired {} for {}",
              expiredBackup.expirationType(),
              HexFormat.of().formatHex(expiredBackup.hashedBackupId()));
          Metrics
              .counter(EXPIRED_BACKUPS_COUNTER_NAME,
                  "tier", expiredBackup.expirationType().name(),
                  "dryRun", String.valueOf(dryRun))
              .increment();
        })
        .onErrorResume(throwable -> {
          logger.warn("Failed to remove tier {} for backup {}",
              expiredBackup.expirationType(),
              HexFormat.of().formatHex(expiredBackup.hashedBackupId()));
          return Mono.just(false);
        });
  }
}
