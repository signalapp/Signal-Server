/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class RemoveExpiredLinkedDevicesCommand extends AbstractSinglePassCrawlAccountsCommand {

  private static final int DEFAULT_MAX_CONCURRENCY = 16;
  private static final int DEFAULT_BUFFER_SIZE = 16_384;
  private static final int DEFAULT_RETRIES = 3;

  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";
  private static final String BUFFER_ARGUMENT = "buffer";
  private static final String RETRIES_ARGUMENT = "retries";

  private static final String REMOVED_DEVICES_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "removedDevices");

  private static final String RETRIED_UPDATES_COUNTER_NAME = name(RemoveExpiredLinkedDevicesCommand.class,
      "retries");

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

    subparser.addArgument("--buffer")
        .type(Integer.class)
        .dest(BUFFER_ARGUMENT)
        .setDefault(DEFAULT_BUFFER_SIZE)
        .help("Accounts to buffer");

    subparser.addArgument("--retries")
        .type(Integer.class)
        .dest(RETRIES_ARGUMENT)
        .setDefault(DEFAULT_RETRIES)
        .help("Maximum number of retries permitted per device");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {

    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final int bufferSize = getNamespace().getInt(BUFFER_ARGUMENT);
    final int maxRetries = getNamespace().getInt(RETRIES_ARGUMENT);

    final Counter successCounter = Metrics.counter(REMOVED_DEVICES_COUNTER_NAME, "dryRun", String.valueOf(dryRun));

    accounts.map(a -> Tuples.of(a, getExpiredLinkedDeviceIds(a.getDevices())))
        .filter(accountAndExpiredDevices -> !accountAndExpiredDevices.getT2().isEmpty())
        .buffer(bufferSize)
        .map(source -> {
          final List<Tuple2<Account, Set<Byte>>> shuffled = new ArrayList<>(source);
          Collections.shuffle(shuffled);
          return shuffled;
        })
        .limitRate(2)
        .flatMapIterable(Function.identity())
        .flatMap(accountAndExpiredDevices -> {
          final Account account = accountAndExpiredDevices.getT1();
          final Set<Byte> expiredDevices = accountAndExpiredDevices.getT2();

          final Mono<Long> accountUpdate = dryRun
              ? Mono.just((long) expiredDevices.size())
              : deleteDevices(account, expiredDevices, maxRetries);

          return accountUpdate
              .doOnNext(successCounter::increment)
              .onErrorResume(t -> {
                logger.warn("Failed to remove expired linked devices for {}", account.getUuid(), t);
                return Mono.empty();
              });
        }, maxConcurrency)
        .then()
        .block();
  }

  private Mono<Long> deleteDevices(final Account account, final Set<Byte> expiredDevices, final int maxRetries) {

    final Counter retryCounter = Metrics.counter(RETRIED_UPDATES_COUNTER_NAME);
    final Counter errorCounter = Metrics.counter(FAILED_UPDATES_COUNTER_NAME);

    return Flux.fromIterable(expiredDevices)
        .flatMap(deviceId ->
                Mono.fromFuture(() -> getCommandDependencies().accountsManager().removeDevice(account, deviceId))
                    .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                        .doAfterRetry(ignored -> retryCounter.increment())
                        .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                    .onErrorResume(t -> {
                      logger.info("Failed to remove expired linked device {}.{}", account.getUuid(), deviceId, t);
                      errorCounter.increment();
                      return Mono.empty();
                    }),
            // limit concurrency to avoid contested updates
            1)
        .count();
  }

  @VisibleForTesting
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
