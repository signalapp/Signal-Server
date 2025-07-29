/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Subparser;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.EncryptDeviceCreationTimestampUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

public class EncryptDeviceCreationTimestampCommand extends AbstractSinglePassCrawlAccountsCommand {
  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  private static final int MAX_CONCURRENCY = 16;

  private static final String PROCESSED_ACCOUNT_COUNTER_NAME =
      name(EncryptDeviceCreationTimestampCommand.class, "processedAccount");

  private static final Logger log = LoggerFactory.getLogger(EncryptDeviceCreationTimestampCommand.class);

  public EncryptDeviceCreationTimestampCommand() {
    super("encrypt-device-creation-timestamps", "Encrypts the creation timestamp of devices on accounts");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually update device records");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean isDryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final Counter processedAccountCounter =
        Metrics.counter(PROCESSED_ACCOUNT_COUNTER_NAME, "dryRun", String.valueOf(isDryRun));
    accounts
        .flatMap(account -> {
          Mono<Void> encryptTimestampMono = isDryRun
              ? Mono.empty()
              : Mono.fromFuture(
                  () -> getCommandDependencies().accountsManager().updateAsync(account, a -> {
                    final IdentityKey aciIdentityKey = account.getIdentityKey(IdentityType.ACI);
                    for (final Device device :  a.getDevices()) {
                      final byte[] createdAtCiphertext = EncryptDeviceCreationTimestampUtil.encrypt(
                          device.getCreated(), aciIdentityKey,
                          device.getId(), device.getRegistrationId(IdentityType.ACI));
                      device.setCreatedAtCiphertext(createdAtCiphertext);
                    }
                  }).thenRun(Util.NOOP));
          return encryptTimestampMono
              .doOnSuccess(_ -> processedAccountCounter.increment())
              .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(4)))
              .onErrorResume(throwable -> {
                log.warn("Failed to encrypt creation timestamps on account {}", account.getUuid(), throwable);
                return Mono.empty();
              });
        }, MAX_CONCURRENCY)
        .then()
        .block();

    log.info("Finished encrypting device timestamps");
  }
}
