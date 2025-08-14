/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;

class RedisDynamoDbMessageStreamTest {

  private MessagesDynamoDb messagesDynamoDb;
  private MessagesCache messagesCache;

  private RedisDynamoDbMessageStream redisDynamoDbMessageStream;

  private Device device;

  private static final UUID ACCOUNT_IDENTIFIER = UUID.randomUUID();
  private static final byte DEVICE_ID = Device.PRIMARY_ID;

  @BeforeEach
  void setUp() {
    messagesDynamoDb = mock(MessagesDynamoDb.class);
    messagesCache = mock(MessagesCache.class);

    device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    redisDynamoDbMessageStream = new RedisDynamoDbMessageStream(messagesDynamoDb,
        messagesCache,
        ACCOUNT_IDENTIFIER,
        device,
        mock(RedisDynamoDbMessagePublisher.class));

    when(messagesDynamoDb.deleteMessage(any(), any(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(messagesCache.remove(any(), anyByte(), any(UUID.class)))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
  }

  @Test
  void acknowledgeMessageDynamoDb() {
    final MessageProtos.Envelope message = generateMessage();
    final UUID messageGuid = UUID.fromString(message.getServerGuid());
    final long serverTimestamp = message.getServerTimestamp();

    when(messagesDynamoDb.deleteMessage(ACCOUNT_IDENTIFIER, device, messageGuid, serverTimestamp))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(message)));

    redisDynamoDbMessageStream.acknowledgeMessage(message).join();

    verify(messagesCache).remove(ACCOUNT_IDENTIFIER, DEVICE_ID, messageGuid);
    verify(messagesDynamoDb).deleteMessage(ACCOUNT_IDENTIFIER, device, messageGuid, serverTimestamp);
  }

  @Test
  void acknowledgeMessageRedis() {
    final MessageProtos.Envelope message = generateMessage();
    final UUID messageGuid = UUID.fromString(message.getServerGuid());

    when(messagesCache.remove(ACCOUNT_IDENTIFIER, DEVICE_ID, messageGuid))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(RemovedMessage.fromEnvelope(message))));

    redisDynamoDbMessageStream.acknowledgeMessage(message).join();

    verify(messagesCache).remove(ACCOUNT_IDENTIFIER, DEVICE_ID, messageGuid);
    verify(messagesDynamoDb, never()).deleteMessage(any(), any(), any(), anyLong());
  }

  private static MessageProtos.Envelope generateMessage() {
    return MessageProtos.Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setDestinationServiceId(new AciServiceIdentifier(ACCOUNT_IDENTIFIER).toServiceIdentifierString())
        .setServerTimestamp(System.currentTimeMillis())
        .setClientTimestamp(System.currentTimeMillis())
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .build();
  }
}
