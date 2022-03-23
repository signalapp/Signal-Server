/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.MessagesDynamoDb;
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
    builder.setSource("12348675309");
    builder.setSourceUuid(UUID.randomUUID().toString());
    builder.setSourceDevice(1);
    builder.setContent(ByteString.copyFromUtf8("MOO"));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp + 1);
    builder.setDestinationUuid(UUID.randomUUID().toString());

    MESSAGE2 = builder.build();

    builder.setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER);
    builder.clearSource();
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

    final List<OutgoingMessageEntity> messagesStored = messagesDynamoDb.load(destinationUuid, destinationDeviceId,
        MessagesDynamoDb.RESULT_SET_CHUNK_SIZE);
    assertThat(messagesStored).isNotNull().hasSize(3);
    final MessageProtos.Envelope firstMessage =
        MESSAGE1.getServerGuid().compareTo(MESSAGE3.getServerGuid()) < 0 ? MESSAGE1 : MESSAGE3;
    final MessageProtos.Envelope secondMessage = firstMessage == MESSAGE1 ? MESSAGE3 : MESSAGE1;
    assertThat(messagesStored).element(0).satisfies(verify(firstMessage));
    assertThat(messagesStored).element(1).satisfies(verify(secondMessage));
    assertThat(messagesStored).element(2).satisfies(verify(MESSAGE2));
  }

  @Test
  void testDeleteForDestination() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE1));
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE3));
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).satisfies(verify(MESSAGE2));

    messagesDynamoDb.deleteAllMessagesForAccount(destinationUuid);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).satisfies(verify(MESSAGE2));
  }

  @Test
  void testDeleteForDestinationDevice() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE1));
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE3));
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).satisfies(verify(MESSAGE2));

    messagesDynamoDb.deleteAllMessagesForDevice(destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE1));
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).satisfies(verify(MESSAGE2));
  }

  @Test
  void testDeleteMessageByDestinationAndGuid() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE1));
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE3));
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).satisfies(verify(MESSAGE2));

    messagesDynamoDb.deleteMessageByDestinationAndGuid(secondDestinationUuid,
        UUID.fromString(MESSAGE2.getServerGuid()));

    assertThat(messagesDynamoDb.load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE1));
    assertThat(messagesDynamoDb.load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).satisfies(verify(MESSAGE3));
    assertThat(messagesDynamoDb.load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();
  }

  private static void verify(OutgoingMessageEntity retrieved, MessageProtos.Envelope inserted) {
    assertThat(retrieved.getTimestamp()).isEqualTo(inserted.getTimestamp());
    assertThat(retrieved.getSource()).isEqualTo(inserted.hasSource() ? inserted.getSource() : null);
    assertThat(retrieved.getSourceUuid()).isEqualTo(inserted.hasSourceUuid() ? UUID.fromString(inserted.getSourceUuid()) : null);
    assertThat(retrieved.getSourceDevice()).isEqualTo(inserted.getSourceDevice());
    assertThat(retrieved.getType()).isEqualTo(inserted.getType().getNumber());
    assertThat(retrieved.getContent()).isEqualTo(inserted.hasContent() ? inserted.getContent().toByteArray() : null);
    assertThat(retrieved.getServerTimestamp()).isEqualTo(inserted.getServerTimestamp());
    assertThat(retrieved.getGuid()).isEqualTo(UUID.fromString(inserted.getServerGuid()));
    assertThat(retrieved.getDestinationUuid()).isEqualTo(UUID.fromString(inserted.getDestinationUuid()));
  }

  private static VerifyMessage verify(MessageProtos.Envelope expected) {
    return new VerifyMessage(expected);
  }

  private static final class VerifyMessage implements Consumer<OutgoingMessageEntity> {

    private final MessageProtos.Envelope expected;

    public VerifyMessage(MessageProtos.Envelope expected) {
      this.expected = expected;
    }

    @Override
    public void accept(OutgoingMessageEntity outgoingMessageEntity) {
      verify(outgoingMessageEntity, expected);
    }
  }
}
