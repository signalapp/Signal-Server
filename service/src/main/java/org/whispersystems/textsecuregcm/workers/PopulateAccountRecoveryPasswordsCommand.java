/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class PopulateAccountRecoveryPasswordsCommand extends AbstractSinglePassCrawlAccountsCommand {

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final String ACCOUNTS_MIGRATED_COUNTER_NAME =
      MetricsUtil.name(PopulateAccountRecoveryPasswordsCommand.class, "accountsMigrated");

  private static final Logger logger = LoggerFactory.getLogger(PopulateAccountRecoveryPasswordsCommand.class);

  public PopulateAccountRecoveryPasswordsCommand() {
    super("populate-account-recovery-passwords", "Copies phone number recovery passwords as account recovery passwords");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually migrate any passwords");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(64)
        .help("Max concurrency for migration operations");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final AccountsManager accountsManager = getCommandDependencies().accountsManager();

    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);

    final Counter accountsMigratedCounter =
        Metrics.counter(ACCOUNTS_MIGRATED_COUNTER_NAME, "dryRun", String.valueOf(dryRun));

      accounts.flatMap(account -> (dryRun
                      ? Mono.<Void>empty()
                      : Mono.<Void>fromRunnable(() -> accountsManager.migrateAccountRecoveryPassword(account)))
              .onErrorResume(throwable -> {
                logger.warn("Failed to migrate account recovery password for account {}",
                    account.getAccountIdentifier(), throwable);

                return Mono.empty();
              })
                      .subscribeOn(Schedulers.boundedElastic())
                      .doOnSuccess(_ -> accountsMigratedCounter.increment()), maxConcurrency)
              .then()
              .block();
  }
}
