/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;

public class AccountCleaner extends AccountDatabaseCrawlerListener {

  private static final Logger log = LoggerFactory.getLogger(AccountCleaner.class);

  private static final String DELETED_ACCOUNT_COUNTER_NAME = name(AccountCleaner.class, "deletedAccounts");
  private static final String DELETION_REASON_TAG_NAME = "reason";

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
    final List<CompletableFuture<Void>> deletionFutures = new ArrayList<>();

    for (Account account : chunkAccounts) {
      if (isExpired(account) || needsExplicitRemoval(account)) {
        final String deletionReason = needsExplicitRemoval(account) ? "newlyExpired" : "previouslyExpired";

        deletionFutures.add(CompletableFuture.runAsync(() -> {
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
                Metrics.counter(DELETED_ACCOUNT_COUNTER_NAME, DELETION_REASON_TAG_NAME, deletionReason).increment();
              }
            }));
      }
    }

    try {
      CompletableFuture.allOf(deletionFutures.toArray(new CompletableFuture[0])).join();
    } catch (final Exception e) {
      log.debug("Failed to delete one or more accounts in chunk", e);
    }
  }

  private boolean needsExplicitRemoval(Account account) {
    return account.getMasterDevice().isPresent()           &&
           hasPushToken(account.getMasterDevice().get())   &&
           isExpired(account);
  }

  private boolean hasPushToken(Device device) {
    return !Util.isEmpty(device.getGcmId()) || !Util.isEmpty(device.getApnId()) || !Util.isEmpty(device.getVoipApnId()) || device.getFetchesMessages();
  }

  private boolean isExpired(Account account) {
    return account.getLastSeen() + TimeUnit.DAYS.toMillis(365) < System.currentTimeMillis();
  }

}
