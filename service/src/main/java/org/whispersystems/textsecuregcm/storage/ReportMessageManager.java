/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.RedisException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ReportMessageManager {

  private final ReportMessageDynamoDb reportMessageDynamoDb;
  private final FaultTolerantRedisClusterClient rateLimitCluster;

  private final Duration counterTtl;

  private final List<ReportedMessageListener> reportedMessageListeners = new ArrayList<>();

  private static final String REPORT_MESSAGE_COUNTER_NAME = MetricsUtil.name(ReportMessageManager.class, "reportMessage");
  private static final String FOUND_MESSAGE_TAG = "foundMessage";
  private static final String TOKEN_PRESENT_TAG = "hasReportSpamToken";

  private static final Logger logger = LoggerFactory.getLogger(ReportMessageManager.class);

  public ReportMessageManager(final ReportMessageDynamoDb reportMessageDynamoDb,
      final FaultTolerantRedisClusterClient rateLimitCluster,
      final Duration counterTtl) {

    this.reportMessageDynamoDb = reportMessageDynamoDb;
    this.rateLimitCluster = rateLimitCluster;

    this.counterTtl = counterTtl;
  }

  public void addListener(final ReportedMessageListener listener) {
    this.reportedMessageListeners.add(listener);
  }

  public void store(String sourceAci, UUID messageGuid) {
    try {
      reportMessageDynamoDb.store(hash(messageGuid, Objects.requireNonNull(sourceAci)));
    } catch (final Exception e) {
      logger.warn("Failed to store hash", e);
    }
  }

  public void report(final Optional<String> sourceNumber,
      final Optional<UUID> sourceAci,
      final Optional<UUID> sourcePni,
      final UUID messageGuid,
      final UUID reporterUuid,
      final Optional<byte[]> reportSpamToken,
      final String reporterUserAgent) {

    final boolean found = sourceAci.map(uuid -> reportMessageDynamoDb.remove(hash(messageGuid, uuid.toString())))
        .orElse(false);

    Metrics.counter(REPORT_MESSAGE_COUNTER_NAME,
            Tags.of(FOUND_MESSAGE_TAG, String.valueOf(found),
                    TOKEN_PRESENT_TAG, String.valueOf(reportSpamToken.isPresent()))
                .and(UserAgentTagUtil.getPlatformTag(reporterUserAgent)))
        .increment();

    if (found) {
      rateLimitCluster.useCluster(connection -> {
        sourcePni.ifPresent(pni -> {
          final String reportedSenderKey = getReportedSenderPniKey(pni);
          connection.sync().pfadd(reportedSenderKey, reporterUuid.toString());
          connection.sync().expire(reportedSenderKey, counterTtl.toSeconds());
        });

        sourceAci.ifPresent(aci -> {
          final String reportedSenderKey = getReportedSenderAciKey(aci);
          connection.sync().pfadd(reportedSenderKey, reporterUuid.toString());
          connection.sync().expire(reportedSenderKey, counterTtl.toSeconds());
        });
      });

      sourceNumber.ifPresent(number ->
          reportedMessageListeners.forEach(listener -> {
            try {
              listener.handleMessageReported(number, messageGuid, reporterUuid, reportSpamToken);
            } catch (final Exception e) {
              logger.error("Failed to notify listener of reported message", e);
            }
          }));
    }
  }

  /**
   * Returns the number of times messages from the given account have been reported by recipients as spam. Note that
   * this method makes a call to an external service, and callers should take care to memoize calls where possible and
   * avoid unnecessary calls.
   *
   * @param account the account to check for recent reports
   * @return the number of times the given number has been reported recently
   */
  public int getRecentReportCount(final Account account) {
    try {
      return rateLimitCluster.withCluster(
          connection ->
              Math.max(
                  connection.sync().pfcount(getReportedSenderPniKey(account.getPhoneNumberIdentifier())).intValue(),
                  connection.sync().pfcount(getReportedSenderAciKey(account.getUuid())).intValue()));
    } catch (final RedisException e) {
      return 0;
    }
  }

  private byte[] hash(UUID messageGuid, String otherId) {
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }

    sha256.update(UUIDUtil.toBytes(messageGuid));
    sha256.update(otherId.getBytes(StandardCharsets.UTF_8));

    return sha256.digest();
  }

  private static String getReportedSenderAciKey(final UUID aci) {
    return "reported_account::" + aci.toString();
  }

  private static String getReportedSenderPniKey(final UUID pni) {
    return "reported_pni::" + pni.toString();
  }
}
