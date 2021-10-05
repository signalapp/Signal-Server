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

  private final FaultTolerantRedisCluster metricsCluster;

  private static final String NORMALIZED_NUMBER_COUNT_KEY = "NonNormalizedAccountCrawlerListener::normalized";
  private static final String NON_NORMALIZED_NUMBER_COUNT_KEY = "NonNormalizedAccountCrawlerListener::nonNormalized";

  private static final PhoneNumberUtil PHONE_NUMBER_UTIL = PhoneNumberUtil.getInstance();

  private static final Logger log = LoggerFactory.getLogger(NonNormalizedAccountCrawlerListener.class);

  public NonNormalizedAccountCrawlerListener(final FaultTolerantRedisCluster metricsCluster) {
    this.metricsCluster = metricsCluster;
  }

  @Override
  public void onCrawlStart() {
    metricsCluster.useCluster(connection -> {
      connection.sync().del(NORMALIZED_NUMBER_COUNT_KEY, NON_NORMALIZED_NUMBER_COUNT_KEY);
    });
  }

  @Override
  protected void onCrawlChunk(final Optional<UUID> fromUuid, final List<Account> chunkAccounts) {

    final int normalizedNumbers;
    final int nonNormalizedNumbers;
    {
      int workingNormalizedNumbers = 0;
      int workingNonNormalizedNumbers = 0;

      for (final Account account : chunkAccounts) {
        if (hasNumberNormalized(account)) {
          workingNormalizedNumbers++;
        } else {
          workingNonNormalizedNumbers++;
        }
      }

      normalizedNumbers = workingNormalizedNumbers;
      nonNormalizedNumbers = workingNonNormalizedNumbers;
    }

    metricsCluster.useCluster(connection -> {
      connection.sync().incrby(NORMALIZED_NUMBER_COUNT_KEY, normalizedNumbers);
      connection.sync().incrby(NON_NORMALIZED_NUMBER_COUNT_KEY, nonNormalizedNumbers);
    });
  }

  @Override
  public void onCrawlEnd(final Optional<UUID> fromUuid) {
    final int normalizedNumbers = metricsCluster.withCluster(connection ->
        Integer.parseInt(connection.sync().get(NORMALIZED_NUMBER_COUNT_KEY)));

    final int nonNormalizedNumbers = metricsCluster.withCluster(connection ->
        Integer.parseInt(connection.sync().get(NON_NORMALIZED_NUMBER_COUNT_KEY)));

    log.info("Crawl completed. Normalized numbers: {}; non-normalized numbers: {}",
        normalizedNumbers, nonNormalizedNumbers);
  }

  @VisibleForTesting
  static boolean hasNumberNormalized(final Account account) {
    try {
      final PhoneNumber phoneNumber = PHONE_NUMBER_UTIL.parse(account.getNumber(), null);
      return account.getNumber().equals(PHONE_NUMBER_UTIL.format(phoneNumber, PhoneNumberFormat.E164));
    } catch (final NumberParseException e) {
      log.warn("Failed to parse phone number for account {}", account.getUuid(), e);
      return false;
    }
  }
}
