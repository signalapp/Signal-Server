package org.whispersystems.textsecuregcm.storage;

import io.lettuce.core.RedisException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ReportMessageManager {

  private final ReportMessageDynamoDb reportMessageDynamoDb;
  private final FaultTolerantRedisCluster rateLimitCluster;

  private final Duration counterTtl;

  private final List<ReportedMessageListener> reportedMessageListeners = new ArrayList<>();

  private static final Logger logger = LoggerFactory.getLogger(ReportMessageManager.class);

  public ReportMessageManager(final ReportMessageDynamoDb reportMessageDynamoDb,
      final FaultTolerantRedisCluster rateLimitCluster,
      final Duration counterTtl) {

    this.reportMessageDynamoDb = reportMessageDynamoDb;
    this.rateLimitCluster = rateLimitCluster;

    this.counterTtl = counterTtl;
  }

  public void addListener(final ReportedMessageListener listener) {
    this.reportedMessageListeners.add(listener);
  }

  public void store(String sourceNumber, UUID messageGuid) {

    try {
      Objects.requireNonNull(sourceNumber);

      reportMessageDynamoDb.store(hash(messageGuid, sourceNumber));
    } catch (final Exception e) {
      logger.warn("Failed to store hash", e);
    }
  }

  public void report(String sourceNumber, UUID messageGuid, UUID reporterUuid) {

    final boolean found = reportMessageDynamoDb.remove(hash(messageGuid, sourceNumber));

    if (found) {
      rateLimitCluster.useCluster(connection -> {
        final String reportedSenderKey = getReportedSenderKey(sourceNumber);

        connection.sync().pfadd(reportedSenderKey, reporterUuid.toString());
        connection.sync().expire(reportedSenderKey, counterTtl.toSeconds());
      });

      reportedMessageListeners.forEach(listener -> {
        try {
          listener.handleMessageReported(sourceNumber, messageGuid, reporterUuid);
        } catch (final Exception e) {
          logger.error("Failed to notify listener of reported message", e);
        }
      });
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
