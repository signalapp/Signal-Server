/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class MigrateSignedECPreKeysCommand extends AbstractSinglePassCrawlAccountsCommand {

  private static final String STORE_KEY_ATTEMPT_COUNTER_NAME =
      MetricsUtil.name(MigrateSignedECPreKeysCommand.class, "storeKeyAttempt");

  // It's tricky to find, but the default connection count for the AWS SDK's async DynamoDB client is 50. As long as
  // we stay below that, we should be fine.
  private static final int DEFAULT_MAX_CONCURRENCY = 32;

  private static final String BUFFER_ARGUMENT = "buffer";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final Logger logger = LoggerFactory.getLogger(MigrateSignedECPreKeysCommand.class);

  public MigrateSignedECPreKeysCommand() {
    super("migrate-signed-ec-pre-keys", "Migrate signed EC pre-keys from Account records to a dedicated table");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");

    subparser.addArgument("--buffer")
        .type(Integer.class)
        .dest(BUFFER_ARGUMENT)
        .setDefault(16_384)
        .help("Devices to buffer");
  }

  @Override
  protected void crawlAccounts(final ParallelFlux<Account> accounts) {
    final KeysManager keysManager = getCommandDependencies().keysManager();
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final int bufferSize = getNamespace().getInt(BUFFER_ARGUMENT);

    accounts
        .sequential()
        .flatMap(account -> Flux.fromIterable(account.getDevices())
            .flatMap(device -> Flux.fromArray(IdentityType.values())
                .filter(identityType -> device.getSignedPreKey(identityType) != null)
                .map(identityType -> Tuples.of(account.getIdentifier(identityType), device.getId(), device.getSignedPreKey(identityType)))))
        .buffer(bufferSize)
        .map(source -> {
          final List<Tuple3<UUID, Byte, ECSignedPreKey>> shuffled = new ArrayList<>(source);
          Collections.shuffle(shuffled);
          return shuffled;
        })
        .flatMapIterable(Function.identity())
        .flatMap(keyTuple -> {
          final UUID identifier = keyTuple.getT1();
          final byte deviceId = keyTuple.getT2();
          final ECSignedPreKey signedPreKey = keyTuple.getT3();

          return Mono.fromFuture(() -> keysManager.storeEcSignedPreKeyIfAbsent(identifier, deviceId, signedPreKey))
              .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)).onRetryExhaustedThrow((spec, rs) -> rs.failure()))
              .onErrorResume(throwable -> {
                logger.warn("Failed to migrate key for UUID {}, device {}", identifier, deviceId);
                return Mono.just(false);
              })
              .doOnSuccess(keyStored -> Metrics.counter(STORE_KEY_ATTEMPT_COUNTER_NAME, "stored", String.valueOf(keyStored)).increment());
        }, maxConcurrency)
        .then()
        .block();
  }
}
