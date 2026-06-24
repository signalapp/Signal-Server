/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;

class AcknowledgementMirroringMessageStreamTest {

  private RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private FoundationDbMessageStream foundationDbMessageStream;

  private AcknowledgementMirroringMessageStream mirroringMessageStream;

  private static long serialMessageTimestamp = 0;

  private enum UUIDVersion {
    V4,
    V8
  }

  @BeforeEach
  void setUp() {
    redisDynamoDbMessageStream = mock(RedisDynamoDbMessageStream.class);

    when(redisDynamoDbMessageStream.acknowledgeMessage(any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));

    foundationDbMessageStream = mock(FoundationDbMessageStream.class);

    when(foundationDbMessageStream.acknowledgeMessage(any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));

    mirroringMessageStream = new AcknowledgementMirroringMessageStream(
        redisDynamoDbMessageStream,
        foundationDbMessageStream);
  }

  @ParameterizedTest
  @EnumSource(UUIDVersion.class)
  void acknowledgeMessages(final UUIDVersion uuidVersion) {
    final List<MessageStreamEntry> entries = new ArrayList<>();
    {
      for (int i = 0; i < AcknowledgementMirroringMessageStream.FOUNDATIONDB_REQUEST_SIZE + 1; i++) {
        entries.add(generateEnvelopeEntry(uuidVersion));
      }

      entries.add(new MessageStreamEntry.QueueEmpty());
    }

    when(redisDynamoDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.fromIterable(entries)));

    when(foundationDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.fromIterable(
            uuidVersion == UUIDVersion.V8 ? entries : Collections.emptyList())));

    JdkFlowAdapter.flowPublisherToFlux(mirroringMessageStream.getMessages())
        .doOnNext(entry -> {
          if (entry instanceof MessageStreamEntry.Envelope(MessageProtos.Envelope message)) {
            mirroringMessageStream.acknowledgeMessage(UUIDUtil.fromByteString(message.getServerGuid()),
                message.getServerTimestamp());
          }
        })
        .takeUntil(entry -> entry instanceof MessageStreamEntry.QueueEmpty)
        .then()
        .block();

    if (uuidVersion == UUIDVersion.V8) {
      entries.stream()
          .filter(streamEntry -> streamEntry instanceof MessageStreamEntry.Envelope)
          .map(streamEntry -> ((MessageStreamEntry.Envelope) streamEntry).message())
          .forEach(envelope -> verify(foundationDbMessageStream).acknowledgeMessage(
              UUIDUtil.fromByteString(envelope.getServerGuid()), envelope.getServerTimestamp()));
    } else {
      verify(foundationDbMessageStream, never()).acknowledgeMessage(any(), anyLong());
    }
  }

  @Test
  void acknowledgeMessagesOverflow() {
    final List<MessageStreamEntry> entries = new ArrayList<>();
    {
      for (int i = 0; i < AcknowledgementMirroringMessageStream.MAX_PENDING_ACKNOWLEDGEMENTS + 1; i++) {
        entries.add(generateEnvelopeEntry(UUIDVersion.V8));
      }

      entries.add(new MessageStreamEntry.QueueEmpty());
    }

    when(redisDynamoDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.fromIterable(entries)));

    when(foundationDbMessageStream.getMessages())
        .thenReturn(JdkFlowAdapter.publisherToFlowPublisher(Flux.empty()));

    assertThrows(IllegalStateException.class,
        () -> JdkFlowAdapter.flowPublisherToFlux(mirroringMessageStream.getMessages())
            .doOnNext(entry -> {
              if (entry instanceof MessageStreamEntry.Envelope(MessageProtos.Envelope message)) {
                mirroringMessageStream.acknowledgeMessage(UUIDUtil.fromByteString(message.getServerGuid()),
                    message.getServerTimestamp());
              }
            })
            .takeUntil(entry -> entry instanceof MessageStreamEntry.QueueEmpty)
            .then()
            .block());

    verify(foundationDbMessageStream, never()).acknowledgeMessage(any(), anyLong());
  }

  private static MessageStreamEntry.Envelope generateEnvelopeEntry(final UUIDVersion uuidVersion) {
    final UUID messageGuid = switch (uuidVersion) {
      case V4 -> UUID.randomUUID();
      case V8 -> MessageGuidUtil.generateRandomV8UUID();
    };

    return new MessageStreamEntry.Envelope(MessageProtos.Envelope.newBuilder()
        .setServerGuid(UUIDUtil.toByteString(messageGuid))
        .setServerTimestamp(serialMessageTimestamp++)
        .build());
  }
}
