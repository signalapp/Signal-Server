/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
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
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class ConcatenatingMessageStreamTest {

  private DynamoDbMessageStream dynamoDbMessageStream;
  private FoundationDbMessageStream foundationDbMessageStream;

  private ConcatenatingMessageStream concatenatingMessageStream;

  @BeforeEach
  void setUp() {
    dynamoDbMessageStream = mock(DynamoDbMessageStream.class);
    foundationDbMessageStream = mock(FoundationDbMessageStream.class);

    concatenatingMessageStream = new ConcatenatingMessageStream(dynamoDbMessageStream, foundationDbMessageStream);
  }

  @Test
  void getMessages() {
    final int messageCount = 3;

    final List<? extends MessageStreamEntry> messageStreamEntries = IntStream.range(0, messageCount)
        .mapToObj(i -> new MessageStreamEntry.Envelope(MessageProtos.Envelope.newBuilder().setServerTimestamp(i).build()))
        .toList();

    for (int i = 0; i < messageCount; i++) {
      when(dynamoDbMessageStream.getMessages())
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

    when(dynamoDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(new MessageStreamEntry.Envelope(MessageProtos.Envelope.newBuilder()
            .setServerGuid(UUIDUtil.toByteString(v4Uuid))
            .setServerTimestamp(serverTimestamp)
            .build()))));

    final UUID v8UUID = MessageGuidUtil.generateRandomV8UUID();

    when(foundationDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(new MessageStreamEntry.Envelope(MessageProtos.Envelope.newBuilder()
            .setServerGuid(UUIDUtil.toByteString(v8UUID))
            .setServerTimestamp(serverTimestamp)
            .build()))));

    JdkFlowAdapter.flowPublisherToFlux(concatenatingMessageStream.getMessages())
        .filter(messageStreamEntry -> messageStreamEntry instanceof MessageStreamEntry.Envelope)
        .cast(MessageStreamEntry.Envelope.class)
        .doOnNext(envelopeEntry -> concatenatingMessageStream.acknowledgeMessage(
            UUIDUtil.fromByteString(envelopeEntry.message().getServerGuid()),
            envelopeEntry.message().getServerTimestamp()))
        .then()
        .block();

    verify(dynamoDbMessageStream).acknowledgeMessage(v4Uuid, serverTimestamp);
    verify(foundationDbMessageStream, never()).acknowledgeMessage(eq(v4Uuid), anyLong());

    verify(dynamoDbMessageStream).acknowledgeMessage(v8UUID, serverTimestamp);
    verify(foundationDbMessageStream).acknowledgeMessage(v8UUID, serverTimestamp);
  }

  @Test
  void acknowledgeMessageDynamoDbEmpty() {
    when(dynamoDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.empty()));

    final UUID v8UUID = MessageGuidUtil.generateRandomV8UUID();
    final long serverTimestamp = System.currentTimeMillis();

    when(foundationDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.just(new MessageStreamEntry.Envelope(MessageProtos.Envelope.newBuilder()
            .setServerGuid(UUIDUtil.toByteString(v8UUID))
            .setServerTimestamp(serverTimestamp)
            .build()))));

    JdkFlowAdapter.flowPublisherToFlux(concatenatingMessageStream.getMessages())
        .filter(messageStreamEntry -> messageStreamEntry instanceof MessageStreamEntry.Envelope)
        .cast(MessageStreamEntry.Envelope.class)
        .doOnNext(envelopeEntry -> concatenatingMessageStream.acknowledgeMessage(
            UUIDUtil.fromByteString(envelopeEntry.message().getServerGuid()),
            envelopeEntry.message().getServerTimestamp()))
        .then()
        .block();

    verify(dynamoDbMessageStream, never()).acknowledgeMessage(any(), anyLong());
    verify(foundationDbMessageStream).acknowledgeMessage(v8UUID, serverTimestamp);
  }
}
