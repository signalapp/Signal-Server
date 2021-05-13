package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ReportMessageManagerTest {

  private final ReportMessageDynamoDb reportMessageDynamoDb = mock(ReportMessageDynamoDb.class);
  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

  private final ReportMessageManager reportMessageManager = new ReportMessageManager(reportMessageDynamoDb, meterRegistry);

  @Test
  void testStore() {

    final UUID messageGuid = UUID.randomUUID();
    final String number = "+15105551111";

    assertThrows(NullPointerException.class, () ->  reportMessageManager.store(null, messageGuid));

    reportMessageManager.store(number, messageGuid);

    verify(reportMessageDynamoDb).store(any());
  }

  @Test
  void testReport() {
    final String sourceNumber = "+15105551111";
    final UUID messageGuid = UUID.randomUUID();

    when(reportMessageDynamoDb.remove(any())).thenReturn(false);
    reportMessageManager.report(sourceNumber, messageGuid);

    assertEquals(0, getCounterTotal(ReportMessageManager.REPORT_COUNTER_NAME));

    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    reportMessageManager.report(sourceNumber, messageGuid);

    assertEquals(1, getCounterTotal(ReportMessageManager.REPORT_COUNTER_NAME));
  }

  private double getCounterTotal(final String counterName) {
    return meterRegistry.find(counterName).counters().stream()
        .map(Counter::count)
        .reduce(Double::sum)
        .orElse(0.0);
  }

}
