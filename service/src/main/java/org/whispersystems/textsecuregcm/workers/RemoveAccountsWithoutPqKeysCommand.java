/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.DevicePlatformUtil;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RemoveAccountsWithoutPqKeysCommand extends AbstractSinglePassCrawlAccountsCommand {

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String RETRIES_ARGUMENT = "retries";

  @VisibleForTesting
  static final String MAX_ACCOUNTS_ARGUMENT = "max-accounts";

  private static final String REMOVED_ACCOUNT_COUNTER_NAME =
      MetricsUtil.name(RemoveAccountsWithoutPqKeysCommand.class, "removedAccount");

  private static final Logger log = LoggerFactory.getLogger(RemoveAccountsWithoutPqKeysCommand.class);

  public RemoveAccountsWithoutPqKeysCommand() {
    super("remove-accounts-without-pq-keys", "Removes accounts with primary devices that don't have PQ keys");
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
        .setDefault(16)
        .help("Max concurrency for DynamoDB operations");

    subparser.addArgument("--retries")
        .type(Integer.class)
        .dest(RETRIES_ARGUMENT)
        .setDefault(3)
        .help("Maximum number of DynamoDB retries permitted per device");

    subparser.addArgument("--max-accounts")
        .type(Integer.class)
        .required(true)
        .dest(MAX_ACCOUNTS_ARGUMENT)
        .help("Maximum number of accounts to remove per run");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final int maxRetries = getNamespace().getInt(RETRIES_ARGUMENT);
    final int maxAccounts = getNamespace().getInt(MAX_ACCOUNTS_ARGUMENT);

    final AccountsManager accountsManager = getCommandDependencies().accountsManager();
    final PqKeysUtil pqKeysUtil = new PqKeysUtil(getCommandDependencies().keysManager(), maxConcurrency, maxRetries);

    accounts
        .transform(pqKeysUtil::getAccountsWithoutPqKeys)
        .take(maxAccounts)
        .filter(accountWithoutPqKeys -> {
          if (!accountWithoutPqKeys.hasLockedCredentials()) {
            log.warn("Account {} is not locked", accountWithoutPqKeys.getIdentifier(IdentityType.ACI));
          }

          return accountWithoutPqKeys.hasLockedCredentials();
        })
        .flatMap(accountWithoutPqKeys -> {
          final String platform = DevicePlatformUtil.getDevicePlatform(accountWithoutPqKeys.getPrimaryDevice())
              .map(Enum::name)
              .orElse("unknown");

          return dryRun
              ? Mono.just(platform)
              : Mono.fromFuture(() -> accountsManager.delete(accountWithoutPqKeys, AccountsManager.DeletionReason.ADMIN_DELETED))
                  .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                      .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                  .thenReturn(platform)
                  .onErrorResume(throwable -> {
                    log.warn("Failed to remove account without PQ keys {}", accountWithoutPqKeys.getIdentifier(IdentityType.ACI), throwable);
                    return Mono.empty();
                  });
        })
        .doOnNext(deletedAccountPlatform -> {
          Metrics.counter(REMOVED_ACCOUNT_COUNTER_NAME,
                  "dryRun", String.valueOf(dryRun),
                  "platform", deletedAccountPlatform)
              .increment();
        })
        .then()
        .block();
  }
}
