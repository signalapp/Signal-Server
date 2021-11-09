/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class NonNormalizedAccountCrawlerListener extends AccountDatabaseCrawlerListener {

  private final AccountsManager accountsManager;
  private final FaultTolerantRedisCluster metricsCluster;

  private static final String NORMALIZED_NUMBER_COUNT_KEY = "NonNormalizedAccountCrawlerListener::normalized";
  private static final String NON_NORMALIZED_NUMBER_COUNT_KEY = "NonNormalizedAccountCrawlerListener::nonNormalized";
  private static final String CONFLICTING_NUMBER_COUNT_KEY = "NonNormalizedAccountCrawlerListener::conflicts";

  private static final PhoneNumberUtil PHONE_NUMBER_UTIL = PhoneNumberUtil.getInstance();

  private static final Logger log = LoggerFactory.getLogger(NonNormalizedAccountCrawlerListener.class);

  public NonNormalizedAccountCrawlerListener(
      final AccountsManager accountsManager,
      final FaultTolerantRedisCluster metricsCluster) {

    this.accountsManager = accountsManager;
    this.metricsCluster = metricsCluster;
  }

  @Override
  public void onCrawlStart() {
    metricsCluster.useCluster(connection ->
        connection.sync().del(NORMALIZED_NUMBER_COUNT_KEY, NON_NORMALIZED_NUMBER_COUNT_KEY, CONFLICTING_NUMBER_COUNT_KEY));
  }

  @Override
  protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts) {

    final int normalizedNumbers;
    final int nonNormalizedNumbers;
    final int conflictingNumbers;
    {
      int workingNormalizedNumbers = 0;
      int workingNonNormalizedNumbers = 0;
      int workingConflictingNumbers = 0;

      for (final Account account : chunkAccounts) {
        if (hasNumberNormalized(account)) {
          workingNormalizedNumbers++;
        } else {
          workingNonNormalizedNumbers++;

          try {
            final Optional<Account> maybeConflictingAccount = accountsManager.getByE164(getNormalizedNumber(account));

            if (maybeConflictingAccount.isPresent()) {
              workingConflictingNumbers++;
              log.info("Normalized form of number for account {} conflicts with number for account {}",
                  account.getUuid(), maybeConflictingAccount.get().getUuid());
            }
          } catch (final NumberParseException e) {
            log.warn("Failed to parse phone number for account {}", account.getUuid(), e);
          }
        }
      }

      normalizedNumbers = workingNormalizedNumbers;
      nonNormalizedNumbers = workingNonNormalizedNumbers;
      conflictingNumbers = workingConflictingNumbers;
    }

    metricsCluster.useCluster(connection -> {
      connection.sync().incrby(NORMALIZED_NUMBER_COUNT_KEY, normalizedNumbers);
      connection.sync().incrby(NON_NORMALIZED_NUMBER_COUNT_KEY, nonNormalizedNumbers);
      connection.sync().incrby(CONFLICTING_NUMBER_COUNT_KEY, conflictingNumbers);
    });
  }

  @Override
  public void onCrawlEnd(final Optional<UUID> fromUuid) {
    final int normalizedNumbers = metricsCluster.withCluster(connection ->
        Integer.parseInt(connection.sync().get(NORMALIZED_NUMBER_COUNT_KEY)));

    final int nonNormalizedNumbers = metricsCluster.withCluster(connection ->
        Integer.parseInt(connection.sync().get(NON_NORMALIZED_NUMBER_COUNT_KEY)));

    final int conflictingNumbers = metricsCluster.withCluster(connection ->
        Integer.parseInt(connection.sync().get(CONFLICTING_NUMBER_COUNT_KEY)));

    log.info("Crawl completed. Normalized numbers: {}; non-normalized numbers: {}; conflicting numbers: {}",
        normalizedNumbers, nonNormalizedNumbers, conflictingNumbers);
  }

  @VisibleForTesting
  static boolean hasNumberNormalized(final Account account) {
    try {
      return account.getNumber().equals(getNormalizedNumber(account));
    } catch (final NumberParseException e) {
      log.warn("Failed to parse phone number for account {}", account.getUuid(), e);
      return false;
    }
  }

  private static String getNormalizedNumber(final Account account) throws NumberParseException {
    final PhoneNumber phoneNumber = PHONE_NUMBER_UTIL.parse(account.getNumber(), null);
    return PHONE_NUMBER_UTIL.format(phoneNumber, PhoneNumberFormat.E164);
  }
}
