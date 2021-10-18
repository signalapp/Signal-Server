package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.RedisException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;

public class ReportMessageManager {

  @VisibleForTesting
  static final String REPORT_COUNTER_NAME = name(ReportMessageManager.class, "reported");

  private final ReportMessageDynamoDb reportMessageDynamoDb;
  private final FaultTolerantRedisCluster rateLimitCluster;
  private final MeterRegistry meterRegistry;

  private final Duration counterTtl;

  private static final Logger logger = LoggerFactory.getLogger(ReportMessageManager.class);

  public ReportMessageManager(final ReportMessageDynamoDb reportMessageDynamoDb,
      final FaultTolerantRedisCluster rateLimitCluster,
      final MeterRegistry meterRegistry,
      final Duration counterTtl) {

    this.reportMessageDynamoDb = reportMessageDynamoDb;
    this.rateLimitCluster = rateLimitCluster;
    this.meterRegistry = meterRegistry;

    this.counterTtl = counterTtl;
  }

  public void store(String sourceNumber, UUID messageGuid) {

    try {
      Objects.requireNonNull(sourceNumber);

      reportMessageDynamoDb.store(hash(messageGuid, sourceNumber));
    } catch (final Exception e) {
      logger.warn("Failed to store hash", e);
    }
  }

  public void report(String sourceNumber, UUID messageGuid) {

    final boolean found = reportMessageDynamoDb.remove(hash(messageGuid, sourceNumber));

    if (found) {
      rateLimitCluster.useCluster(connection -> {
        final String reportedSenderKey = getReportedSenderKey(sourceNumber);

        connection.sync().pfadd(reportedSenderKey, sourceNumber);
        connection.sync().expire(reportedSenderKey, counterTtl.toSeconds());
      });

      Counter.builder(REPORT_COUNTER_NAME)
          .tag("countryCode", Util.getCountryCode(sourceNumber))
          .register(meterRegistry)
          .increment();
    }
  }

  /**
   * Returns the number of times messages from the given number have been reported by recipients as abusive. Note that
   * this method makes a call to an external service, and callers should take care to memoize calls where possible and
   * avoid unnecessary calls.
   *
   * @param number the number to check for recent reports
   *
   * @return the number of times the given number has been reported recently
   */
  public int getRecentReportCount(final String number) {
    try {
      return rateLimitCluster.withCluster(connection -> connection.sync().pfcount(getReportedSenderKey(number)).intValue());
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

  private static String getReportedSenderKey(final String senderNumber) {
    return "reported_number::" + senderNumber;
  }
}
