package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.lettuce.core.RedisException;
import io.micrometer.core.instrument.Metrics;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ReportMessageManager {

  private static final String MIGRATION_COUNTER_NAME = name(ReportMessageManager.class, "migration");

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

  public void store(String sourceAci, UUID messageGuid) {

    try {
      Objects.requireNonNull(sourceAci);

      reportMessageDynamoDb.store(hash(messageGuid, sourceAci));
    } catch (final Exception e) {
      logger.warn("Failed to store hash", e);
    }
  }

  public void report(Optional<String> sourceNumber, Optional<UUID> sourceAci, Optional<UUID> sourcePni,
      UUID messageGuid, UUID reporterUuid) {

    // TODO sourceNumber can be removed after 2022-04-15
    final boolean foundByNumber = sourceNumber.map(number -> reportMessageDynamoDb.remove(hash(messageGuid, number)))
        .orElse(false);

    final boolean foundByAci = sourceAci.map(uuid -> reportMessageDynamoDb.remove(hash(messageGuid, uuid.toString()))).
        orElse(false);

    if (foundByNumber || foundByAci) {
      rateLimitCluster.useCluster(connection -> {
        sourceNumber.ifPresent(number -> {
          final String reportedSenderKey = getReportedSenderKey(number);
          connection.sync().pfadd(reportedSenderKey, reporterUuid.toString());
          connection.sync().expire(reportedSenderKey, counterTtl.toSeconds());
        });

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
              listener.handleMessageReported(number, messageGuid, reporterUuid);
            } catch (final Exception e) {
              logger.error("Failed to notify listener of reported message", e);
            }
          }));
    }

    Metrics.counter(
        MIGRATION_COUNTER_NAME,
        "foundByNumber", String.valueOf(foundByNumber),
        "foundByAci", String.valueOf(foundByAci),
        "sourceAciPresent", String.valueOf(sourceAci.isPresent()),
        "sourcePniPresent", String.valueOf(sourcePni.isPresent()),
        "sourceNumberPresent", String.valueOf(sourceNumber.isPresent())
    ).increment();
  }

  /**
   * Returns the number of times messages from the given account have been reported by recipients as abusive. Note that
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
                  Math.max(
                      // TODO number can be removed after 2022-04-15
                      connection.sync().pfcount(getReportedSenderKey(account.getNumber())).intValue(),
                      connection.sync().pfcount(getReportedSenderPniKey(account.getPhoneNumberIdentifier()))
                          .intValue()),
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

  private static String getReportedSenderKey(final String senderNumber) {
    return "reported_number::" + senderNumber;
  }

  private static String getReportedSenderAciKey(final UUID aci) {
    return "reported_account::" + aci.toString();
  }

  private static String getReportedSenderPniKey(final UUID pni) {
    return "reported_pni::" + pni.toString();
  }
}
