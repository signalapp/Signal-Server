/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class DynamoDbMessageStreamTest {

  private MessagesDynamoDb messagesDynamoDb;
  private DynamoDbMessageStream dynamoDbMessageStream;

  private Device device;

  private static final UUID ACCOUNT_IDENTIFIER = UUID.randomUUID();
  private static final byte DEVICE_ID = Device.PRIMARY_ID;

  @BeforeEach
  void setUp() {
    messagesDynamoDb = mock(MessagesDynamoDb.class);

    device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);

    dynamoDbMessageStream = new DynamoDbMessageStream(messagesDynamoDb,
        ACCOUNT_IDENTIFIER,
        device);

    when(messagesDynamoDb.deleteMessage(any(), any(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
  }

  @Test
  void acknowledgeMessageDynamoDb() {
    final MessageProtos.Envelope message = generateMessage();
    final UUID messageGuid = UUIDUtil.fromByteString(message.getServerGuid());
    final long serverTimestamp = message.getServerTimestamp();

    when(messagesDynamoDb.deleteMessage(ACCOUNT_IDENTIFIER, device, messageGuid, serverTimestamp))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(message)));

    dynamoDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp).join();

    verify(messagesDynamoDb).deleteMessage(ACCOUNT_IDENTIFIER, device, messageGuid, serverTimestamp);
  }

  private static MessageProtos.Envelope generateMessage() {
    return MessageProtos.Envelope.newBuilder()
        .setServerGuid(UUIDUtil.toByteString(UUID.randomUUID()))
        .setDestinationServiceId(new AciServiceIdentifier(ACCOUNT_IDENTIFIER).toCompactByteString())
        .setServerTimestamp(System.currentTimeMillis())
        .setClientTimestamp(System.currentTimeMillis())
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .build();
  }
}
