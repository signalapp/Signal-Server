/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

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

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class ReportMessageManagerTest {

  private ReportMessageDynamoDb reportMessageDynamoDb;

  private ReportMessageManager reportMessageManager;

  private String sourceNumber;
  private UUID sourceAci;
  private UUID sourcePni;
  private Account sourceAccount;
  private UUID messageGuid;
  private UUID reporterUuid;

  @RegisterExtension
  static RedisClusterExtension RATE_LIMIT_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @BeforeEach
  void setUp() {
    reportMessageDynamoDb = mock(ReportMessageDynamoDb.class);

    reportMessageManager = new ReportMessageManager(reportMessageDynamoDb,
        RATE_LIMIT_CLUSTER_EXTENSION.getRedisCluster(), Duration.ofDays(1));

    sourceNumber = "+15105551111";
    sourceAci = UUID.randomUUID();
    sourcePni = UUID.randomUUID();
    messageGuid = UUID.randomUUID();
    reporterUuid = UUID.randomUUID();

    sourceAccount = mock(Account.class);
    when(sourceAccount.getUuid()).thenReturn(sourceAci);
    when(sourceAccount.getNumber()).thenReturn(sourceNumber);
    when(sourceAccount.getPhoneNumberIdentifier()).thenReturn(sourcePni);
  }

  @Test
  void testStore() {
    assertDoesNotThrow(() -> reportMessageManager.store(null, messageGuid));

    verifyNoInteractions(reportMessageDynamoDb);

    reportMessageManager.store(sourceAci.toString(), messageGuid);

    verify(reportMessageDynamoDb).store(any());

    doThrow(RuntimeException.class)
        .when(reportMessageDynamoDb).store(any());

    assertDoesNotThrow(() -> reportMessageManager.store(sourceAci.toString(), messageGuid));
  }

  @Test
  void testReport() {
    final ReportedMessageListener listener = mock(ReportedMessageListener.class);
    reportMessageManager.addListener(listener);

    when(reportMessageDynamoDb.remove(any())).thenReturn(false);
    reportMessageManager.report(Optional.of(sourceNumber), Optional.of(sourceAci), Optional.of(sourcePni), messageGuid,
        reporterUuid);

    assertEquals(0, reportMessageManager.getRecentReportCount(sourceAccount));

    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    reportMessageManager.report(Optional.of(sourceNumber), Optional.of(sourceAci), Optional.of(sourcePni), messageGuid,
        reporterUuid);

    assertEquals(1, reportMessageManager.getRecentReportCount(sourceAccount));
    verify(listener).handleMessageReported(sourceNumber, messageGuid, reporterUuid);
  }

  @Test
  void testReportMultipleReporters() {
    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    assertEquals(0, reportMessageManager.getRecentReportCount(sourceAccount));

    for (int i = 0; i < 100; i++) {
      reportMessageManager.report(Optional.of(sourceNumber), Optional.of(sourceAci), Optional.of(sourcePni),
          messageGuid, UUID.randomUUID());
    }

    assertTrue(reportMessageManager.getRecentReportCount(sourceAccount) > 10);
  }

  @Test
  void testReportSingleReporter() {
    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    assertEquals(0, reportMessageManager.getRecentReportCount(sourceAccount));

    for (int i = 0; i < 100; i++) {
      reportMessageManager.report(Optional.of(sourceNumber), Optional.of(sourceAci), Optional.of(sourcePni),
          messageGuid,
          reporterUuid);
    }

    assertEquals(1, reportMessageManager.getRecentReportCount(sourceAccount));
  }

  @Test
  void testReportMultipleReportersByPni() {
    when(reportMessageDynamoDb.remove(any())).thenReturn(true);
    assertEquals(0, reportMessageManager.getRecentReportCount(sourceAccount));

    for (int i = 0; i < 100; i++) {
      reportMessageManager.report(Optional.of(sourceNumber), Optional.empty(), Optional.of(sourcePni),
          messageGuid, UUID.randomUUID());
    }

    reportMessageManager.report(Optional.empty(), Optional.of(sourceAci), Optional.empty(),
        messageGuid, UUID.randomUUID());

    final int recentReportCount = reportMessageManager.getRecentReportCount(sourceAccount);
    assertTrue(recentReportCount > 10);
  }
}
