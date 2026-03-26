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
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import java.time.Duration;

public class RemoveNonSpqrLinkedDevicesCommand extends AbstractSinglePassCrawlAccountsCommand {

  static final String MAX_CONCURRENCY_ARGUMENT = "maxConcurrency";
  static final String DRY_RUN_ARGUMENT = "dryRun";

  private static final String REMOVE_DEVICE_COUNTER_NAME =
      MetricsUtil.name(RemoveNonSpqrLinkedDevicesCommand.class, "removeLinkedDevice");

  private static final Logger logger = LoggerFactory.getLogger(RemoveNonSpqrLinkedDevicesCommand.class);

  public RemoveNonSpqrLinkedDevicesCommand() {
    super("remove-non-spqr-linked-devices", "Removes linked devices that do not support SPQR");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

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
        .help("If true, don't actually remove linked devices");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);

    final AccountsManager accountsManager = getCommandDependencies().accountsManager();

    final Counter removeDeviceCounterName =
        Metrics.counter(REMOVE_DEVICE_COUNTER_NAME, "dryRun", String.valueOf(dryRun));

    accounts
        .flatMap(account -> Flux.fromIterable(account.getDevices())
            .filter(device -> !device.isPrimary())
            .filter(device -> !device.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET))
            .map(device -> Tuples.of(account, device.getId())))
        .flatMap(accountAndDeviceId -> {
          final Mono<Void> removeDeviceMono = dryRun
              ? Mono.empty()
              : Mono.fromRunnable(() -> accountsManager.removeDevice(accountAndDeviceId.getT1(), accountAndDeviceId.getT2()))
                  .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                  .onErrorResume(throwable -> {
                    logger.warn("Failed to remove device: {}:{}",
                        accountAndDeviceId.getT1().getIdentifier(IdentityType.ACI),
                        accountAndDeviceId.getT2(),
                        throwable);

                    return Mono.empty();
                  })
                  .then();

          return removeDeviceMono
              .doOnSuccess(_ -> removeDeviceCounterName.increment());
        }, maxConcurrency)
        .then()
        .block();
  }
}
