/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RegenerateAccountConstraintDataCommand extends AbstractSinglePassCrawlAccountsCommand {

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String RETRIES_ARGUMENT = "retries";

  private static final String PROCESSED_ACCOUNTS_COUNTER_NAME =
      MetricsUtil.name(RegenerateAccountConstraintDataCommand.class, "processedAccounts");

  public RegenerateAccountConstraintDataCommand() {
    super("regenerate-account-constraint-data", "Regenerates account constraint data from a core account table");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually write constraint data");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(16)
        .help("Max concurrency for DynamoDB operations");

    subparser.addArgument("--retries")
        .type(Integer.class)
        .dest(RETRIES_ARGUMENT)
        .setDefault(8)
        .help("Maximum number of DynamoDB retries permitted per account");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accountRecords) {
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final int maxRetries = getNamespace().getInt(RETRIES_ARGUMENT);

    final Accounts accounts = getCommandDependencies().accounts();

    final Counter processedAccountsCounter = Metrics.counter(PROCESSED_ACCOUNTS_COUNTER_NAME,
        "dryRun", String.valueOf(dryRun));

    accountRecords
        .doOnNext(ignored -> processedAccountsCounter.increment())
        .flatMap(account -> dryRun
                ? Mono.empty()
                : Mono.fromFuture(() -> accounts.regenerateConstraints(account))
                    .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(4))
                        .onRetryExhaustedThrow((spec, rs) -> rs.failure())),
            maxConcurrency)
        .then()
        .block();
  }
}
