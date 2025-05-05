/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.shaded.reactor.util.function.Tuples;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.DevicePlatformUtil;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RemoveLinkedDevicesWithoutPniKeysCommand extends AbstractSinglePassCrawlAccountsCommand {

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String RETRIES_ARGUMENT = "retries";

  private static final String REMOVED_DEVICE_COUNTER_NAME =
      MetricsUtil.name(RemoveLinkedDevicesWithoutPniKeysCommand.class, "removedDevice");

  private static final Logger log = LoggerFactory.getLogger(RemoveLinkedDevicesWithoutPniKeysCommand.class);

  public RemoveLinkedDevicesWithoutPniKeysCommand() {
    super("remove-linked-devices-without-pni-keys", "Removes linked devices that do not have PNI signed pre-keys");
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
        .filter(account -> !account.hasLockedCredentials())
        .filter(account -> account.getDevices().size() > 1)
        .flatMap(account -> Flux.fromIterable(account.getDevices())
            .filter(device -> !device.isPrimary())
            .map(device -> Tuples.of(account, device)))
        .flatMap(accountAndDevice -> {
              final Account account = accountAndDevice.getT1();
              final Device device = accountAndDevice.getT2();

              return Mono.fromFuture(
                      () -> keysManager.getEcSignedPreKey(account.getIdentifier(IdentityType.PNI), device.getId()))
                  .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                      .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                  .onErrorResume(throwable -> {
                    log.warn("Failed to get PNI signed pre-key presence for account/device: {}:{}",
                        account.getIdentifier(IdentityType.ACI), device.getId());
                    return Mono.empty();
                  })
                  .map(maybeEcSignedPreKey -> Tuples.of(account, device, maybeEcSignedPreKey));
            }, maxConcurrency)
        .filter(tuple -> tuple.getT3().isEmpty())
        .flatMap(accountAndDevice -> {
          final Account account = accountAndDevice.getT1();
          final Device device = accountAndDevice.getT2();

          return dryRun
              ? Mono.just(device)
              : Mono.fromFuture(() -> accountsManager.removeDevice(account, device.getId()))
                  .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                      .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                  .onErrorResume(throwable -> {
                    log.warn("Failed to remove device: {}:{}", account.getIdentifier(IdentityType.ACI), device.getId());
                    return Mono.empty();
                  })
                  .then(Mono.just(device));
        }, maxConcurrency)
        .doOnNext(removedDevice -> Metrics.counter(REMOVED_DEVICE_COUNTER_NAME,
                "dryRun", String.valueOf(dryRun),
                "platform", DevicePlatformUtil.getDevicePlatform(removedDevice).map(Enum::name).orElse("unknown"))
            .increment())
        .then()
        .block();
  }
}
