/*
 * Copyright 2024 Signal Messenger, LLC
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RemoveExpiredUsernameHoldsCommand extends AbstractSinglePassCrawlAccountsCommand {

  private final Clock clock;

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";
  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  private static final String DELETED_HOLDS_COUNTER_NAME =
      name(RemoveExpiredUsernameHoldsCommand.class, "expiredHolds");

  private static final String INSPECTED_ACCOUNTS_COUNTER_NAME =
      name(RemoveExpiredUsernameHoldsCommand.class, "inspectedAccounts");

  private static final Logger log = LoggerFactory.getLogger(RemoveExpiredUsernameHoldsCommand.class);

  public RemoveExpiredUsernameHoldsCommand(final Clock clock) {
    super("remove-expired-username-holds", "Removes expired username holds from account records");
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
        .help("If true, don't actually delete holds");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean isDryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);

    final Counter deletedHoldsCounter =
        Metrics.counter(DELETED_HOLDS_COUNTER_NAME, "dryRun", String.valueOf(isDryRun));

    final AccountsManager accountManager = getCommandDependencies().accountsManager();
    final AtomicLong accountsInspected = new AtomicLong();
    accounts.flatMap(account -> {
          accountsInspected.incrementAndGet();
          final List<Account.UsernameHold> holds = new ArrayList<>(account.getUsernameHolds());
          final int holdsToRemove = removeExpired(holds);
          final Mono<Void> purgeMono = isDryRun || holdsToRemove == 0
              ? Mono.empty()
              : Mono.fromFuture(() ->
                  accountManager.updateAsync(account, a -> a.setUsernameHolds(holds)).thenRun(Util.NOOP));
          Metrics.counter(INSPECTED_ACCOUNTS_COUNTER_NAME,
                  "dryRun", String.valueOf(isDryRun),
                  "expiredHolds", String.valueOf(holdsToRemove > 0))
              .increment();
          return purgeMono
              .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
              .doOnSuccess(ignored -> deletedHoldsCounter.increment(holdsToRemove))
              .onErrorResume(throwable -> {
                log.warn("Failed to purge {} expired holds on account {}", holdsToRemove, account.getUuid());
                return Mono.empty();
              });
        }, maxConcurrency)
        .then().block();
    log.info("Finished crawl of {} accounts", accountsInspected.get());
  }

  @VisibleForTesting
  int removeExpired(final List<Account.UsernameHold> holds) {
    final Instant now = this.clock.instant();
    int holdsToRemove = 0;
    for (Iterator<Account.UsernameHold> it = holds.iterator(); it.hasNext(); ) {
      if (it.next().expirationSecs() < now.getEpochSecond()) {
        holdsToRemove++;
        it.remove();
      }
    }
    return holdsToRemove;
  }
}

