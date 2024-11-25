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
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;

public class MigrateRegistrationRecoveryPasswordsCommand extends AbstractCommandWithDependencies {

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final String RECORDS_INSPECTED_COUNTER_NAME =
      MetricsUtil.name(MigrateRegistrationRecoveryPasswordsCommand.class, "recordsInspected");

  private static final String RECORDS_MIGRATED_COUNTER_NAME =
      MetricsUtil.name(MigrateRegistrationRecoveryPasswordsCommand.class, "recordsMigrated");

  private static final String DRY_RUN_TAG = "dryRun";

  private static final Logger logger = LoggerFactory.getLogger(MigrateRegistrationRecoveryPasswordsCommand.class);

  public MigrateRegistrationRecoveryPasswordsCommand() {

    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "migrate-registration-recovery-passwords", "Migrate e164-based registration recovery passwords to PNI-based records");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually modify accounts with expired linked devices");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {

    final boolean dryRun = namespace.getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = namespace.getInt(MAX_CONCURRENCY_ARGUMENT);

    final Counter recordsInspectedCounter =
        Metrics.counter(RECORDS_INSPECTED_COUNTER_NAME, DRY_RUN_TAG, String.valueOf(dryRun));

    final Counter recordsMigratedCounter =
        Metrics.counter(RECORDS_MIGRATED_COUNTER_NAME, DRY_RUN_TAG, String.valueOf(dryRun));

    final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager =
        commandDependencies.registrationRecoveryPasswordsManager();

    registrationRecoveryPasswordsManager.getE164AssociatedRegistrationRecoveryPasswords()
        .doOnNext(tuple -> recordsInspectedCounter.increment())
        .flatMap(tuple -> {
          final String e164 = tuple.getT1();
          final SaltedTokenHash saltedTokenHash = tuple.getT2();
          final long expiration = tuple.getT3();

          return dryRun
              ? Mono.just(false)
              : Mono.fromFuture(() -> registrationRecoveryPasswordsManager.migrateE164Record(e164, saltedTokenHash, expiration))
                  .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                  .onErrorResume(throwable -> {
                        logger.warn("Failed to migrate record for {}", e164, throwable);
                        return Mono.empty();
                      });
        }, maxConcurrency)
        .filter(migrated -> migrated)
        .doOnNext(ignored -> recordsMigratedCounter.increment())
        .then()
        .block();
  }
}
