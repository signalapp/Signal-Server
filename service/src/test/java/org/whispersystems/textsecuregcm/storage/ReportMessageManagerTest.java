package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class ReportMessageManagerTest {

  private ReportMessageDynamoDb reportMessageDynamoDb;
  private MeterRegistry meterRegistry;

  private ReportMessageManager reportMessageManager;

  @RegisterExtension
  static RedisClusterExtension RATE_LIMIT_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @BeforeEach
  void setUp() {
    reportMessageDynamoDb = mock(ReportMessageDynamoDb.class);
    meterRegistry = new SimpleMeterRegistry();

    reportMessageManager = new ReportMessageManager(reportMessageDynamoDb,
        RATE_LIMIT_CLUSTER_EXTENSION.getRedisCluster(), meterRegistry, Duration.ofDays(1));
  }

  @Test
  void testStore() {

    final UUID messageGuid = UUID.randomUUID();
    final String number = "+15105551111";

    assertDoesNotThrow(() -> reportMessageManager.store(null, messageGuid));

    verifyNoInteractions(reportMessageDynamoDb);

    reportMessageManager.store(number, messageGuid);

    verify(reportMessageDynamoDb).store(any());

    doThrow(RuntimeException.class)
        .when(reportMessageDynamoDb).store(any());

    assertDoesNotThrow(() -> reportMessageManager.store(number, messageGuid));
  }

  @Test
  void testReport() {
    final String sourceNumber = "+15105551111";
    final UUID messageGuid = UUID.randomUUID();
    final UUID reporterUuid = UUID.randomUUID();

    when(reportMessageDynamoDb.remove(any())).thenReturn(false);
    reportMessageManager.report(sourceNumber, messageGuid, reporterUuid);

    assertEquals(0, getCounterTotal(ReportMessageManager.REPORT_COUNTER_NAME));
    assertEquals(0, reportMessageManager.getRecentReportCount(sourceNumber));

    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    reportMessageManager.report(sourceNumber, messageGuid, reporterUuid);

    assertEquals(1, getCounterTotal(ReportMessageManager.REPORT_COUNTER_NAME));
    assertEquals(1, reportMessageManager.getRecentReportCount(sourceNumber));
  }

  private double getCounterTotal(final String counterName) {
    return meterRegistry.find(counterName).counters().stream()
        .map(Counter::count)
        .reduce(Double::sum)
        .orElse(0.0);
  }

  @Test
  void testReportMultipleReporters() {
    final String sourceNumber = "+15105551111";
    final UUID messageGuid = UUID.randomUUID();

    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    assertEquals(0, reportMessageManager.getRecentReportCount(sourceNumber));

    for (int i = 0; i < 100; i++) {
      reportMessageManager.report(sourceNumber, messageGuid, UUID.randomUUID());
    }

    assertTrue(reportMessageManager.getRecentReportCount(sourceNumber) > 10);
  }

  @Test
  void testReportSingleReporter() {
    final String sourceNumber = "+15105551111";
    final UUID messageGuid = UUID.randomUUID();
    final UUID reporterUuid = UUID.randomUUID();

    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    assertEquals(0, reportMessageManager.getRecentReportCount(sourceNumber));

    for (int i = 0; i < 100; i++) {
      reportMessageManager.report(sourceNumber, messageGuid, reporterUuid);
    }

    assertEquals(1, reportMessageManager.getRecentReportCount(sourceNumber));
  }
}
