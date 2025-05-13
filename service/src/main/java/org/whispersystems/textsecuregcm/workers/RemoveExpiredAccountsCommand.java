/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

public class RemoveExpiredAccountsCommand extends AbstractSinglePassCrawlAccountsCommand {

  private final Clock clock;

  static final Duration MAX_IDLE_DURATION = Duration.ofDays(120);

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  private static final int MAX_CONCURRENCY = 16;

  private static final String DELETED_ACCOUNT_COUNTER_NAME =
      name(RemoveExpiredAccountsCommand.class, "deletedAccounts");

  private static final Logger log = LoggerFactory.getLogger(RemoveExpiredAccountsCommand.class);

  public RemoveExpiredAccountsCommand(final Clock clock) {
    super("remove-expired-accounts", "Removes all accounts that have been idle for more than a set period of time");

    this.clock = clock;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually delete accounts");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean isDryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final Counter deletedAccountCounter =
        Metrics.counter(DELETED_ACCOUNT_COUNTER_NAME, "dryRun", String.valueOf(isDryRun));

    accounts.filter(this::isExpired)
        .flatMap(expiredAccount -> {
          final Mono<Void> deleteAccountMono = isDryRun
              ? Mono.empty()
              : Mono.fromFuture(() -> getCommandDependencies().accountsManager().delete(expiredAccount, AccountsManager.DeletionReason.EXPIRED));

          return deleteAccountMono
              .doOnSuccess(ignored -> deletedAccountCounter.increment())
              .retryWhen(Retry.backoff(8, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(4)))
              .onErrorResume(throwable -> {
                log.warn("Failed to delete account {}", expiredAccount.getUuid(), throwable);
                return Mono.empty();
              });
        }, MAX_CONCURRENCY)
        .then()
        .block();
  }

  @VisibleForTesting
  boolean isExpired(final Account account) {
    return Instant.ofEpochMilli(account.getLastSeen()).plus(MAX_IDLE_DURATION).isBefore(clock.instant());
  }
}
