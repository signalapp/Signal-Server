/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.shaded.reactor.util.function.Tuples;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RemoveExpiredLinkedDevicesCommand extends AbstractSinglePassCrawlAccountsCommand {

  private static final int MAX_CONCURRENCY = 16;

  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String REMOVED_DEVICES_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "removedDevices");
  private static final String UPDATED_ACCOUNTS_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "updatedAccounts");

  private static final String FAILED_ACCOUNT_UPDATES_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "failedAccountUpdates");
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
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {

    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);

    accounts.map(a -> Tuples.of(a, getExpiredLinkedDeviceIds(a.getDevices())))
        .filter(accountAndExpiredDevices -> !accountAndExpiredDevices.getT2().isEmpty())
        .flatMap(accountAndExpiredDevices -> {
          final Account account = accountAndExpiredDevices.getT1();
          final Set<Byte> expiredDevices = accountAndExpiredDevices.getT2();

          final Mono<Void> accountUpdate = dryRun
              ? Mono.empty()
              : deleteDevices(account, expiredDevices);

          return accountUpdate.thenReturn(expiredDevices.size())
              .onErrorResume(t -> {
                logger.warn("Failed to remove expired linked devices {}", account.getUuid(), t);
                Metrics.counter(FAILED_ACCOUNT_UPDATES_COUNTER_NAME).increment();
                return Mono.empty();
              });

        }, MAX_CONCURRENCY)
        .doOnNext(removedDevices -> {
          Metrics.counter(REMOVED_DEVICES_COUNTER_NAME, "dryRun", String.valueOf(dryRun)).increment(removedDevices);
          Metrics.counter(UPDATED_ACCOUNTS_COUNTER_NAME, "dryRun", String.valueOf(dryRun)).increment();
        })
        .then()
        .block();
  }

  private Mono<Void> deleteDevices(final Account account, final Set<Byte> expiredDevices) {
    return Flux.fromIterable(expiredDevices)
        .flatMap(deviceId ->
                Mono.fromFuture(() -> getCommandDependencies().accountsManager().removeDevice(account, deviceId)),
            // limit concurrency to avoid contested updates
            1)
        .then();
  }

  protected static Set<Byte> getExpiredLinkedDeviceIds(List<Device> devices) {
    return devices.stream()
        // linked devices
        .filter(Predicate.not(Device::isPrimary))
        // that are expired
        .filter(Device::isExpired)
        .map(Device::getId)
        .collect(Collectors.toSet());
  }
}
