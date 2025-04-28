/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

class PqKeysUtil {

  private final KeysManager keysManager;
  private final int maxConcurrency;
  private final int maxRetries;

  private static final Logger log = LoggerFactory.getLogger(PqKeysUtil.class);

  PqKeysUtil(final KeysManager keysManager, final int maxConcurrency, final int maxRetries) {
    this.keysManager = keysManager;
    this.maxConcurrency = maxConcurrency;
    this.maxRetries = maxRetries;
  }

  public Flux<Account> getAccountsWithoutPqKeys(final Flux<Account> accounts) {
    return accounts.flatMap(account -> Mono.fromFuture(
                    () -> keysManager.getLastResort(account.getIdentifier(IdentityType.ACI), Device.PRIMARY_ID))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofSeconds(1))
                    .onRetryExhaustedThrow((spec, rs) -> rs.failure()))
                .onErrorResume(throwable -> {
                  log.warn("Failed to get last-resort key for {}", account.getIdentifier(IdentityType.ACI), throwable);
                  return Mono.empty();
                })
                .filter(Optional::isEmpty)
                .map(ignored -> account),
            maxConcurrency);
  }
}
