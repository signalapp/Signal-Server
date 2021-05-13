package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;

public class ReportMessageManager {

  @VisibleForTesting
  static final String REPORT_COUNTER_NAME = "reported";

  private final ReportMessageDynamoDb reportMessageDynamoDb;
  private final MeterRegistry meterRegistry;

  private static final Logger logger = LoggerFactory.getLogger(ReportMessageManager.class);

  public ReportMessageManager(ReportMessageDynamoDb reportMessageDynamoDb, final MeterRegistry meterRegistry) {

    this.reportMessageDynamoDb = reportMessageDynamoDb;
    this.meterRegistry = meterRegistry;
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
      Counter.builder(REPORT_COUNTER_NAME)
          .tag("countryCode", Util.getCountryCode(sourceNumber))
          .register(meterRegistry)
          .increment();
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
}
