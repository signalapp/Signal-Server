/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountCleaner extends AccountDatabaseCrawlerListener {

  private static final Logger log = LoggerFactory.getLogger(AccountCleaner.class);

  private static final Counter DELETED_ACCOUNT_COUNTER = Metrics.counter(name(AccountCleaner.class, "deletedAccounts"));

  private final AccountsManager accountsManager;
  private final Executor deletionExecutor;

  public AccountCleaner(final AccountsManager accountsManager, final Executor deletionExecutor) {
    this.accountsManager = accountsManager;
    this.deletionExecutor = deletionExecutor;
  }

  @Override
  public void onCrawlStart() {
  }

  @Override
  public void onCrawlEnd(Optional<UUID> fromUuid) {
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) {
    final List<CompletableFuture<Void>> deletionFutures = chunkAccounts.stream()
        .filter(AccountCleaner::isExpired)
        .map(account -> CompletableFuture.runAsync(() -> {
              try {
                accountsManager.delete(account, AccountsManager.DeletionReason.EXPIRED);
              } catch (final InterruptedException e) {
                throw new CompletionException(e);
              }
            }, deletionExecutor)
            .whenComplete((ignored, throwable) -> {
              if (throwable != null) {
                log.warn("Failed to delete account {}", account.getUuid(), throwable);
              } else {
                DELETED_ACCOUNT_COUNTER.increment();
              }
            }))
        .toList();

    try {
      CompletableFuture.allOf(deletionFutures.toArray(new CompletableFuture[0])).join();
    } catch (final Exception e) {
      log.debug("Failed to delete one or more accounts in chunk", e);
    }
  }

  private static boolean isExpired(Account account) {
    return account.getLastSeen() + TimeUnit.DAYS.toMillis(365) < System.currentTimeMillis();
  }

}
