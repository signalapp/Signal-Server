/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class ConcatenatingMessageStreamTest {

  private RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private FoundationDbMessageStream foundationDbMessageStream;

  private ConcatenatingMessageStream concatenatingMessageStream;

  @BeforeEach
  void setUp() {
    redisDynamoDbMessageStream = mock(RedisDynamoDbMessageStream.class);
    foundationDbMessageStream = mock(FoundationDbMessageStream.class);

    concatenatingMessageStream = new ConcatenatingMessageStream(redisDynamoDbMessageStream, foundationDbMessageStream);
  }

  @Test
  void getMessages() {
    final int messageCount = 3;

    final List<? extends MessageStreamEntry> messageStreamEntries = IntStream.range(0, messageCount)
        .mapToObj(i -> new MessageStreamEntry.Envelope(MessageProtos.Envelope.newBuilder().setServerTimestamp(i).build()))
        .toList();

    for (int i = 0; i < messageCount; i++) {
      when(redisDynamoDbMessageStream.getMessages())
          .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.fromIterable(messageStreamEntries.subList(0, i))));

      when(foundationDbMessageStream.getMessages())
          .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.fromIterable(messageStreamEntries.subList(i, messageStreamEntries.size()))));

      StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(concatenatingMessageStream.getMessages()))
          .expectNextSequence(messageStreamEntries)
          .expectComplete()
          .verify();
    }
  }

  @Test
  void acknowledgeMessage() {
    final UUID v4Uuid = UUID.randomUUID();
    final long serverTimestamp = System.currentTimeMillis();

    concatenatingMessageStream.acknowledgeMessage(v4Uuid, serverTimestamp).join();
    verify(redisDynamoDbMessageStream).acknowledgeMessage(v4Uuid, serverTimestamp);
    verify(foundationDbMessageStream, never()).acknowledgeMessage(eq(v4Uuid), anyLong());

    final UUID v8Uuid = MessageGuidUtil.generateRandomV8UUID();

    concatenatingMessageStream.acknowledgeMessage(v8Uuid, serverTimestamp).join();
    verify(redisDynamoDbMessageStream).acknowledgeMessage(v8Uuid, serverTimestamp);
    verify(foundationDbMessageStream).acknowledgeMessage(v8Uuid, serverTimestamp);
  }
}
