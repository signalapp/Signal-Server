/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.tests.util.MessageHelper;
import org.whispersystems.textsecuregcm.tests.util.MessagesDynamoDbExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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

  private ExecutorService messageDeletionExecutorService;
  private MessagesDynamoDb messagesDynamoDb;


  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = MessagesDynamoDbExtension.build();

  @BeforeEach
  void setup() {
    messageDeletionExecutorService = Executors.newSingleThreadExecutor();
    messagesDynamoDb = new MessagesDynamoDb(dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getDynamoDbAsyncClient(), MessagesDynamoDbExtension.TABLE_NAME, Duration.ofDays(14),
        messageDeletionExecutorService);
  }

  @AfterEach
  void teardown() throws Exception {
    messageDeletionExecutorService.shutdown();
    messageDeletionExecutorService.awaitTermination(5, TimeUnit.SECONDS);

    StepVerifier.resetDefaultTimeout();
  }

  @Test
  void testSimpleFetchAfterInsert() {
    final UUID destinationUuid = UUID.randomUUID();
    final int destinationDeviceId = random.nextInt(255) + 1;
    messagesDynamoDb.store(List.of(MESSAGE1, MESSAGE2, MESSAGE3), destinationUuid, destinationDeviceId);

    final List<MessageProtos.Envelope> messagesStored = load(destinationUuid, destinationDeviceId,
        MessagesDynamoDb.RESULT_SET_CHUNK_SIZE);
    assertThat(messagesStored).isNotNull().hasSize(3);
    final MessageProtos.Envelope firstMessage =
        MESSAGE1.getServerGuid().compareTo(MESSAGE3.getServerGuid()) < 0 ? MESSAGE1 : MESSAGE3;
    final MessageProtos.Envelope secondMessage = firstMessage == MESSAGE1 ? MESSAGE3 : MESSAGE1;
    assertThat(messagesStored).element(0).isEqualTo(firstMessage);
    assertThat(messagesStored).element(1).isEqualTo(secondMessage);
    assertThat(messagesStored).element(2).isEqualTo(MESSAGE2);
  }

  @ParameterizedTest
  @ValueSource(ints = {10, 100, 100, 1_000, 3_000})
  void testLoadManyAfterInsert(final int messageCount) {
    final UUID destinationUuid = UUID.randomUUID();
    final int destinationDeviceId = random.nextInt(255) + 1;

    final List<MessageProtos.Envelope> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      messages.add(MessageHelper.createMessage(UUID.randomUUID(), 1, destinationUuid, (i + 1L) * 1000, "message " + i));
    }

    messagesDynamoDb.store(messages, destinationUuid, destinationDeviceId);

    final Publisher<?> fetchedMessages = messagesDynamoDb.load(destinationUuid, destinationDeviceId, null);

    final long firstRequest = Math.min(10, messageCount);
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(15));

    StepVerifier.Step<?> step = StepVerifier.create(fetchedMessages, 0)
        .expectSubscription()
        .thenRequest(firstRequest)
        .expectNextCount(firstRequest);

    if (messageCount > firstRequest) {
      step = step.thenRequest(messageCount)
          .expectNextCount(messageCount - firstRequest);
    }

    step.thenCancel()
        .verify();
  }

  @Test
  void testLimitedLoad() {
    final int messageCount = 200;
    final UUID destinationUuid = UUID.randomUUID();
    final int destinationDeviceId = random.nextInt(255) + 1;

    final List<MessageProtos.Envelope> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      messages.add(MessageHelper.createMessage(UUID.randomUUID(), 1, destinationUuid, (i + 1L) * 1000, "message " + i));
    }

    messagesDynamoDb.store(messages, destinationUuid, destinationDeviceId);

    final int messageLoadLimit = 100;
    final int halfOfMessageLoadLimit = messageLoadLimit / 2;
    final Publisher<?> fetchedMessages = messagesDynamoDb.load(destinationUuid, destinationDeviceId, messageLoadLimit);

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(10));

    final AtomicInteger messagesRemaining = new AtomicInteger(messageLoadLimit);

    StepVerifier.create(fetchedMessages, 0)
        .expectSubscription()
        .thenRequest(halfOfMessageLoadLimit)
        .expectNextCount(halfOfMessageLoadLimit)
        // the first 100 should be fetched and buffered, but further requests should fail
        .then(() -> dynamoDbExtension.stopServer())
        .thenRequest(halfOfMessageLoadLimit)
        .expectNextCount(halfOfMessageLoadLimit)
        // we’ve consumed all the buffered messages, so a single request will fail
        .thenRequest(1)
        .expectError()
        .verify();
  }

  @Test
  void testDeleteForDestination() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteAllMessagesForAccount(destinationUuid);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);
  }

  @Test
  void testDeleteForDestinationDevice() {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteAllMessagesForDevice(destinationUuid, 2);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().isEmpty();
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);
  }

  @Test
  void testDeleteMessageByDestinationAndGuid() throws Exception {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    final Optional<MessageProtos.Envelope> deletedMessage = messagesDynamoDb.deleteMessageByDestinationAndGuid(
        secondDestinationUuid,
        UUID.fromString(MESSAGE2.getServerGuid())).get(5, TimeUnit.SECONDS);

    assertThat(deletedMessage).isPresent();

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();

    final Optional<MessageProtos.Envelope> alreadyDeletedMessage = messagesDynamoDb.deleteMessageByDestinationAndGuid(
        secondDestinationUuid,
        UUID.fromString(MESSAGE2.getServerGuid())).get(5, TimeUnit.SECONDS);

    assertThat(alreadyDeletedMessage).isNotPresent();

  }

  @Test
  void testDeleteSingleMessage() throws Exception {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, 1);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, 2);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteMessage(secondDestinationUuid, 1,
        UUID.fromString(MESSAGE2.getServerGuid()), MESSAGE2.getServerTimestamp()).get(1, TimeUnit.SECONDS);

    assertThat(load(destinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, 2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, 1, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();
  }

  private List<MessageProtos.Envelope> load(final UUID destinationUuid, final long destinationDeviceId,
      final int count) {
    return Flux.from(messagesDynamoDb.load(destinationUuid, destinationDeviceId, count))
        .take(count, true)
        .collectList()
        .block();
  }
}
