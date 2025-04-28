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
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class RemoveLinkedDevicesWithoutPqKeysCommand extends AbstractSinglePassCrawlAccountsCommand {

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String RETRIES_ARGUMENT = "retries";

  private static final String REMOVED_DEVICE_COUNTER_NAME =
      MetricsUtil.name(RemoveLinkedDevicesWithoutPqKeysCommand.class, "removedDevice");

  private static final Logger log = LoggerFactory.getLogger(RemoveLinkedDevicesWithoutPqKeysCommand.class);

  public RemoveLinkedDevicesWithoutPqKeysCommand() {
    super("remove-linked-devices-without-pq-keys", "Removes linked devices that don't have PQ keys");
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
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final int maxRetries = getNamespace().getInt(RETRIES_ARGUMENT);

    final AccountsManager accountsManager = getCommandDependencies().accountsManager();
    final KeysManager keysManager = getCommandDependencies().keysManager();

    accounts
        .filter(account -> account.getDevices().size() > 1)
        .flatMap(
            account -> Mono.fromFuture(() -> keysManager.getPqEnabledDevices(account.getIdentifier(IdentityType.ACI)))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                    .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                .onErrorResume(throwable -> {
                  log.warn("Failed to get PQ key presence for account: {}", account.getIdentifier(IdentityType.ACI));
                  return Mono.empty();
                })
                .flatMapMany(pqEnabledDeviceIds -> Flux.fromIterable(account.getDevices())
                    .filter(device -> !device.isPrimary())
                    .filter(device -> !pqEnabledDeviceIds.contains(device.getId()))
                    .map(device -> Tuples.of(account, device))), maxConcurrency)
        .flatMap(accountAndDevice -> dryRun
            ? Mono.just(accountAndDevice.getT2())
            : Mono.fromFuture(() -> accountsManager.removeDevice(accountAndDevice.getT1(), accountAndDevice.getT2().getId()))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                    .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                .onErrorResume(throwable -> {
                  log.warn("Failed to remove linked device without PQ keys: {}:{}",
                      accountAndDevice.getT1().getIdentifier(IdentityType.ACI), accountAndDevice.getT2().getId());

                  return Mono.empty();
                })
                .map(ignored -> accountAndDevice.getT2()), maxConcurrency)
        .doOnNext(removedDevice -> Metrics.counter(REMOVED_DEVICE_COUNTER_NAME,
                "dryRun", String.valueOf(dryRun),
                "platform", DevicePlatformUtil.getDevicePlatform(removedDevice).map(Enum::name).orElse("unknown"))
            .increment())
        .then()
        .block();
  }
}
