/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.shaded.reactor.util.function.Tuple2;
import io.micrometer.shaded.reactor.util.function.Tuples;
import java.util.Collection;
import java.util.function.Predicate;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RemoveExpiredLinkedDevicesCommand extends AbstractSinglePassCrawlAccountsCommand {

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final String REMOVED_DEVICES_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "removedDevices");

  private static final String FAILED_UPDATES_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "failedUpdates");
  private static final Logger logger = LoggerFactory.getLogger(RemoveExpiredLinkedDevicesCommand.class);

  public RemoveExpiredLinkedDevicesCommand() {
    super("remove-expired-devices", "Removes expired linked devices");
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
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {

    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);

    final Counter successCounter = Metrics.counter(REMOVED_DEVICES_COUNTER_NAME, "dryRun", String.valueOf(dryRun));
    final Counter errorCounter = Metrics.counter(FAILED_UPDATES_COUNTER_NAME);

    accounts.flatMap(account -> Flux.fromIterable(getExpiredLinkedDeviceIds(account)))
        .flatMap(accountAndExpiredDeviceId -> {
          final Account account = accountAndExpiredDeviceId.getT1();
          final byte deviceId = accountAndExpiredDeviceId.getT2();

          Mono<Account> removeDevice = dryRun
              ? Mono.just(account)
              : Mono.fromFuture(() -> getCommandDependencies().accountsManager().removeDevice(account, deviceId));

          return removeDevice.doOnNext(ignored -> successCounter.increment())
              .onErrorResume(t -> {
                logger.warn("Failed to remove expired linked device {}.{}", account.getUuid(), deviceId, t);
                errorCounter.increment();
                return Mono.empty();
              });
        }, maxConcurrency)
        .then()
        .block();
  }

  @VisibleForTesting
  protected static Collection<Tuple2<Account, Byte>> getExpiredLinkedDeviceIds(Account account) {
    return account.getDevices().stream()
        // linked devices
        .filter(Predicate.not(Device::isPrimary))
        // that are expired
        .filter(Device::isExpired)
        .map(device -> Tuples.of(account, device.getId()))
        .toList();
  }
}
