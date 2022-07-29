/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.tests.util.MessagesDynamoDbExtension;

class MessagesDynamoDbTest {


  private static final Random random = new Random();
  private static final MessageProtos.Envelope MESSAGE1;
  private static final MessageProtos.Envelope MESSAGE2;
  private static final MessageProtos.Envelope MESSAGE3;

  static {
    final long serverTimestamp = System.currentTimeMillis();
    MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder();
    builder.setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER);
    builder.setTimestamp(123456789L);
    builder.setContent(ByteString.copyFrom(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp);
    builder.setDestinationUuid(UUID.randomUUID().toString());

    MESSAGE1 = builder.build();

    builder.setType(MessageProtos.Envelope.Type.CIPHERTEXT);
    builder.setSourceUuid(UUID.randomUUID().toString());
    builder.setSourceDevice(1);
    builder.setContent(ByteString.copyFromUtf8("MOO"));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp + 1);
    builder.setDestinationUuid(UUID.randomUUID().toString());

    MESSAGE2 = builder.build();

    builder.setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER);
    builder.clearSourceUuid();
    builder.clearSourceDevice();
    builder.setContent(ByteString.copyFromUtf8("COW"));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp);  // Test same millisecond arrival for two different messages
    builder.setDestinationUuid(UUID.randomUUID().toString());

    MESSAGE3 = builder.build();
  }

  private MessagesDynamoDb messagesDynamoDb;


  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = MessagesDynamoDbExtension.build();

  @BeforeEach
  void setup() {
    messagesDynamoDb = new MessagesDynamoDb(dynamoDbExtension.getDynamoDbClient(), MessagesDynamoDbExtension.TABLE_NAME,
        Duration.ofDays(14));
  }

  @Test
  void testSimpleFetchAfterInsert() {
    final UUID destinationUuid = UUID.randomUUID();
    final int destinationDeviceId = random.nextInt(255) + 1;
    messagesDynamoDb.store(List.of(MESSAGE1, MESSAGE2, MESSAGE3), destinationUuid, destinationDeviceId);

    final List<MessageProtos.Envelope> messagesStored = messagesDynamoDb.load(destinationUuid, destinationDeviceId,
        MessagesDynamoDb.RESULT_SET_CHUNK_SIZE);
    assertThat(messagesStored).isNotNull().hasSize(3);
    final MessageProtos.Envelope firstMessage =
        MESSAGE1.getServerGuid().compareTo(MESSAGE3.getServerGuid()) < 0 ? MESSAGE1 : MESSAGE3;
    final MessageProtos.Envelope secondMessage = firstMessage == MESSAGE1 ? MESSAGE3 : MESSAGE1;
    assertThat(messagesStored).element(0).isEqualTo(firstMessage);
    assertThat(messagesStored).element(1).isEqualTo(secondMessage);
    assertThat(messagesStored).element(2).isEqualTo(MESSAGE2);
  }

  @Test
  void testDeleteForDestination() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteAllMessagesForAccount(destinationUuid);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);
  }

  @Test
  void testDeleteForDestinationDevice() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteAllMessagesForDevice(destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);
  }

  @Test
  void testDeleteMessageByDestinationAndGuid() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteMessageByDestinationAndGuid(secondDestinationUuid,
        UUID.fromString(MESSAGE2.getServerGuid()));

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();
  }

  @Test
  void testDeleteSingleMessage() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteMessage(secondDestinationUuid, 1,
        UUID.fromString(MESSAGE2.getServerGuid()), MESSAGE2.getServerTimestamp());

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();
  }
}
