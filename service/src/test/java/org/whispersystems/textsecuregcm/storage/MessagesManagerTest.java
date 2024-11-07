/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import reactor.core.publisher.Mono;

class MessagesManagerTest {

  private final MessagesDynamoDb messagesDynamoDb = mock(MessagesDynamoDb.class);
  private final MessagesCache messagesCache = mock(MessagesCache.class);
  private final ReportMessageManager reportMessageManager = mock(ReportMessageManager.class);

  private final MessagesManager messagesManager = new MessagesManager(messagesDynamoDb, messagesCache,
      reportMessageManager, Executors.newSingleThreadExecutor());

  @Test
  void insert() {
    final UUID sourceAci = UUID.randomUUID();
    final Envelope message = Envelope.newBuilder()
        .setSourceServiceId(sourceAci.toString())
        .build();

    final UUID destinationUuid = UUID.randomUUID();

    messagesManager.insert(destinationUuid, Device.PRIMARY_ID, message);

    verify(reportMessageManager).store(eq(sourceAci.toString()), any(UUID.class));

    final Envelope syncMessage = Envelope.newBuilder(message)
        .setSourceServiceId(destinationUuid.toString())
        .build();

    messagesManager.insert(destinationUuid, Device.PRIMARY_ID, syncMessage);

    verifyNoMoreInteractions(reportMessageManager);
  }

  @ParameterizedTest
  @CsvSource({
      "false, false, false",
      "false, true, true",
      "true, false, true",
      "true, true, true"
  })
  void mayHaveMessages(final boolean hasCachedMessages, final boolean hasPersistedMessages, final boolean expectMayHaveMessages) {
    final UUID accountIdentifier = UUID.randomUUID();
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    when(messagesCache.hasMessagesAsync(accountIdentifier, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(hasCachedMessages));

    when(messagesDynamoDb.mayHaveMessages(accountIdentifier, device))
        .thenReturn(CompletableFuture.completedFuture(hasPersistedMessages));

    if (hasCachedMessages) {
      verifyNoInteractions(messagesDynamoDb);
    }

    assertEquals(expectMayHaveMessages, messagesManager.mayHaveMessages(accountIdentifier, device).join());
  }

  @ParameterizedTest
  @CsvSource({
      ",,",
      "1,,1",
      ",1,1",
      "2,1,1",
      "1,2,2"
  })
  public void oldestMessageTimestamp(Long oldestCached, Long oldestPersisted, Long expected) {
    final UUID accountIdentifier = UUID.randomUUID();
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    when(messagesCache.getEarliestUndeliveredTimestamp(accountIdentifier, Device.PRIMARY_ID))
        .thenReturn(oldestCached == null ? Mono.empty() : Mono.just(oldestCached));
    when(messagesDynamoDb.load(accountIdentifier, device, 1))
        .thenReturn(oldestPersisted == null
            ? Mono.empty()
            : Mono.just(Envelope.newBuilder().setServerTimestamp(oldestPersisted).build()));
    final Optional<Instant> earliest =
        messagesManager.getEarliestUndeliveredTimestampForDevice(accountIdentifier, device).join();
    assertEquals(Optional.ofNullable(expected).map(Instant::ofEpochMilli), earliest);
  }
}
