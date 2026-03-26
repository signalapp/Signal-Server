/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;

public class RemoveNonSpqrAccountsCommand extends AbstractSinglePassCrawlAccountsCommand {

  static final String MAX_ACCOUNTS_ARGUMENT = "maxAccounts";
  static final String MAX_CONCURRENCY_ARGUMENT = "maxConcurrency";
  static final String DRY_RUN_ARGUMENT = "dryRun";

  private static final String REMOVE_ACCOUNT_COUNTER_NAME =
      MetricsUtil.name(RemoveNonSpqrAccountsCommand.class, "removeAccount");

  private static final Logger logger = LoggerFactory.getLogger(RemoveNonSpqrAccountsCommand.class);

  public RemoveNonSpqrAccountsCommand() {
    super("remove-non-spqr-accounts", "Removes accounts whose primary devices do not support SPQR");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--max-accounts")
        .type(Integer.class)
        .dest(MAX_ACCOUNTS_ARGUMENT)
        .required(true)
        .help("Max accounts to remove in a single run of this command");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(32)
        .help("Max concurrency for DynamoDB operations");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually remove accounts");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final int maxAccounts = getNamespace().getInt(MAX_ACCOUNTS_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);

    final AccountsManager accountsManager = getCommandDependencies().accountsManager();

    final Counter removeAccountCounterName =
        Metrics.counter(REMOVE_ACCOUNT_COUNTER_NAME, "dryRun", String.valueOf(dryRun));

    accounts
        .filter(account -> !account.getPrimaryDevice().hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET))
        .take(maxAccounts)
        .flatMap(account -> {
          final Mono<Void> removeAccountMono = dryRun
              ? Mono.empty()
              : Mono.fromRunnable(() -> accountsManager.delete(account, AccountsManager.DeletionReason.ADMIN_DELETED))
                  .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                  .onErrorResume(throwable -> {
                    logger.warn("Failed to remove account: {}",
                        account.getIdentifier(IdentityType.ACI),
                        throwable);

                    return Mono.empty();
                  })
                  .then();

          return removeAccountMono
              .doOnSuccess(_ -> removeAccountCounterName.increment());
        }, maxConcurrency)
        .then()
        .block();
  }
}
