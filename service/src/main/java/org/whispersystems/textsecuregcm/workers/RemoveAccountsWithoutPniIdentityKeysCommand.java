/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
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
import java.time.Duration;

public class RemoveAccountsWithoutPniIdentityKeysCommand extends AbstractSinglePassCrawlAccountsCommand {

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String RETRIES_ARGUMENT = "retries";

  private static final String REMOVED_ACCOUNT_COUNTER_NAME =
      MetricsUtil.name(RemoveAccountsWithoutPniIdentityKeysCommand.class, "removedAccount");

  private static final Logger log = LoggerFactory.getLogger(RemoveAccountsWithoutPniIdentityKeysCommand.class);

  public RemoveAccountsWithoutPniIdentityKeysCommand() {
    super("remove-accounts-without-pni-identity-keys", "Deletes accounts without PNI identity keys");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually lock accounts with expired linked devices");

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
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final int maxRetries = getNamespace().getInt(RETRIES_ARGUMENT);

    final AccountsManager accountsManager = getCommandDependencies().accountsManager();

    accounts
        .filter(account -> account.getIdentityKey(IdentityType.PNI) == null)
        .filter(accountWithoutPniIdentityKey -> {
          if (!accountWithoutPniIdentityKey.hasLockedCredentials()) {
            log.warn("Account {} is not locked", accountWithoutPniIdentityKey.getIdentifier(IdentityType.ACI));
            return false;
          }

          return true;
        })
        .flatMap(accountWithoutPniIdentityKey -> {
          final String platform = DevicePlatformUtil.getDevicePlatform(accountWithoutPniIdentityKey.getPrimaryDevice())
              .map(Enum::name)
              .orElse("unknown");

          return dryRun
              ? Mono.just(platform)
              : Mono.fromFuture(() -> accountsManager.delete(accountWithoutPniIdentityKey, AccountsManager.DeletionReason.ADMIN_DELETED))
                  .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                      .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                  .thenReturn(platform)
                  .onErrorResume(throwable -> {
                    log.warn("Failed to delete account without PNI identity key: {}",
                        accountWithoutPniIdentityKey.getIdentifier(IdentityType.ACI), throwable);

                    return Mono.empty();
                  });
        }, maxConcurrency)
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
