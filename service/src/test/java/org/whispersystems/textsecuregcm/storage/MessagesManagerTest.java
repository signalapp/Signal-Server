/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;

class MessagesManagerTest {

  private final MessagesDynamoDb messagesDynamoDb = mock(MessagesDynamoDb.class);
  private final MessagesCache messagesCache = mock(MessagesCache.class);
  private final PushLatencyManager pushLatencyManager = mock(PushLatencyManager.class);
  private final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);

  private final MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache,
      pushLatencyManager, reportMessageManager);

  @Test
  void insert() {
    final String sourceNumber = "+12025551212";
    final UUID sourceAci = UUID.randomUUID();
    final Envelope message = Envelope.newBuilder()
        .setSource(sourceNumber)
        .setSourceUuid(sourceAci.toString())
        .build();

    final UUID destinationUuid = UUID.randomUUID();

    messagesManager.insert(destinationUuid, 1L, message);

    verify(reportMessageManager).store(eq(sourceAci.toString()), any(UUID.class));

    final Envelope syncMessage = Envelope.newBuilder(message)
        .setSourceUuid(destinationUuid.toString())
        .build();

    messagesManager.insert(destinationUuid, 1L, syncMessage);

    verifyNoMoreInteractions(reportMessageManager);
  }
}
