/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class UnlinkDevicesWithIdlePrimaryCommand extends AbstractSinglePassCrawlAccountsCommand {

  private final Clock clock;

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String PRIMARY_IDLE_DAYS_ARGUMENT = "primary-idle-days";

  @VisibleForTesting
  static final int DEFAULT_PRIMARY_IDLE_DAYS = 90;

  private static final String UNLINK_DEVICE_COUNTER_NAME =
      MetricsUtil.name(UnlinkDevicesWithIdlePrimaryCommand.class, "unlinkDevice");

  private static final Logger logger = LoggerFactory.getLogger(UnlinkDevicesWithIdlePrimaryCommand.class);

  public UnlinkDevicesWithIdlePrimaryCommand(final Clock clock) {
    super("unlink-devices-with-idle-primary", "Unlinks linked devices if the account's primary device is idle");

    this.clock = clock;
  }

  @Override
  public void configure(final Subparser subparser) {
    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually delete accounts");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(16)
        .help("Max concurrency for DynamoDB operations");

    subparser.addArgument("--primary-idle-days")
        .type(Integer.class)
        .dest(PRIMARY_IDLE_DAYS_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_PRIMARY_IDLE_DAYS)
        .help("The number of inactivity after which a primary device is considered idle");

    super.configure(subparser);
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean isDryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final Duration idleDurationThreshold = Duration.ofDays(getNamespace().getInt(PRIMARY_IDLE_DAYS_ARGUMENT));

    final AccountsManager accountsManager = getCommandDependencies().accountsManager();

    final Counter unlinkDeviceCounter =
        Metrics.counter(UNLINK_DEVICE_COUNTER_NAME, "dryRun", String.valueOf(isDryRun));

    final Instant currentTime = clock.instant();

    accounts
        .filter(account -> isPrimaryDeviceIdle(account, currentTime, idleDurationThreshold))
        .flatMap(accountWithIdlePrimaryDevice -> Flux.fromIterable(accountWithIdlePrimaryDevice.getDevices())
            .filter(device -> !device.isPrimary())
            .map(linkedDevice -> Tuples.of(accountWithIdlePrimaryDevice, linkedDevice.getId())))
        .flatMap(accountAndLinkedDeviceId -> {
          final Mono<Account> unlinkDeviceMono = isDryRun
              ? Mono.empty()
              : Mono.fromFuture(() -> accountsManager.removeDevice(accountAndLinkedDeviceId.getT1(), accountAndLinkedDeviceId.getT2()));

          return unlinkDeviceMono
              .doOnSuccess(ignored -> unlinkDeviceCounter.increment())
              .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(4)))
              .onErrorResume(throwable -> {
                logger.warn("Failed to unlink device to delete account {}:{}", accountAndLinkedDeviceId.getT1().getIdentifier(
                    IdentityType.ACI), accountAndLinkedDeviceId.getT2(), throwable);

                return Mono.empty();
              });
        }, maxConcurrency)
        .then()
        .block();
  }

  private static boolean isPrimaryDeviceIdle(final Account account, final Instant currentTime, final Duration idleDurationThreshold) {
    final Duration durationSincePrimaryLastSeen =
        Duration.between(Instant.ofEpochMilli(account.getPrimaryDevice().getLastSeen()), currentTime);

    return durationSincePrimaryLastSeen.compareTo(idleDurationThreshold) > 0;
  }
}
