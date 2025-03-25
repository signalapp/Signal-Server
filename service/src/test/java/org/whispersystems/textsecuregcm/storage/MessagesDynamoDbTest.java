/*
 * Copyright 2013 Signal Messenger, LLC
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
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.tests.util.MessageHelper;
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
    builder.setClientTimestamp(123456789L);
    builder.setContent(ByteString.copyFrom(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp);
    builder.setDestinationServiceId(UUID.randomUUID().toString());

    MESSAGE1 = builder.build();

    builder.setType(MessageProtos.Envelope.Type.CIPHERTEXT);
    builder.setSourceServiceId(UUID.randomUUID().toString());
    builder.setSourceDevice(1);
    builder.setContent(ByteString.copyFromUtf8("MOO"));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp + 1);
    builder.setDestinationServiceId(UUID.randomUUID().toString());

    MESSAGE2 = builder.build();

    builder.setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER);
    builder.clearSourceDevice();
    builder.clearSourceDevice();
    builder.setContent(ByteString.copyFromUtf8("COW"));
    builder.setServerGuid(UUID.randomUUID().toString());
    builder.setServerTimestamp(serverTimestamp);  // Test same millisecond arrival for two different messages
    builder.setDestinationServiceId(UUID.randomUUID().toString());

    MESSAGE3 = builder.build();
  }

  private ExecutorService messageDeletionExecutorService;
  private MessagesDynamoDb messagesDynamoDb;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.MESSAGES);

  @BeforeEach
  void setup() {
    messageDeletionExecutorService = Executors.newSingleThreadExecutor();
    messagesDynamoDb = new MessagesDynamoDb(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.MESSAGES.tableName(), Duration.ofDays(14),
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
    final byte destinationDeviceId = (byte) (random.nextInt(Device.MAXIMUM_DEVICE_ID) + 1);
    final Device destinationDevice = DevicesHelper.createDevice(destinationDeviceId);

    messagesDynamoDb.store(List.of(MESSAGE1, MESSAGE2, MESSAGE3), destinationUuid, destinationDevice);

    final List<MessageProtos.Envelope> messagesStored = load(destinationUuid, destinationDevice,
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
    final byte destinationDeviceId = (byte) (random.nextInt(Device.MAXIMUM_DEVICE_ID) + 1);
    final Device destinationDevice = DevicesHelper.createDevice(destinationDeviceId);

    final List<MessageProtos.Envelope> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      messages.add(MessageHelper.createMessage(UUID.randomUUID(), Device.PRIMARY_ID, destinationUuid, (i + 1L) * 1000,
          "message " + i));
    }

    messagesDynamoDb.store(messages, destinationUuid, destinationDevice);

    final Publisher<?> fetchedMessages = messagesDynamoDb.load(destinationUuid, destinationDevice, null);

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
    final byte destinationDeviceId = (byte) (random.nextInt(Device.MAXIMUM_DEVICE_ID) + 1);
    final Device destinationDevice = DevicesHelper.createDevice(destinationDeviceId);

    final List<MessageProtos.Envelope> messages = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      messages.add(MessageHelper.createMessage(UUID.randomUUID(), Device.PRIMARY_ID, destinationUuid, (i + 1L) * 1000,
          "message " + i));
    }

    messagesDynamoDb.store(messages, destinationUuid, destinationDevice);

    final int messageLoadLimit = 100;
    final int halfOfMessageLoadLimit = messageLoadLimit / 2;
    final Publisher<?> fetchedMessages = messagesDynamoDb.load(destinationUuid, destinationDevice,
        messageLoadLimit);

    StepVerifier.setDefaultTimeout(Duration.ofSeconds(10));

    final AtomicInteger messagesRemaining = new AtomicInteger(messageLoadLimit);

    StepVerifier.create(fetchedMessages, 0)
        .expectSubscription()
        .thenRequest(halfOfMessageLoadLimit)
        .expectNextCount(halfOfMessageLoadLimit)
        // the first 100 should be fetched and buffered, but further requests should fail
        .then(DYNAMO_DB_EXTENSION::stopServer)
        .thenRequest(halfOfMessageLoadLimit)
        .expectNextCount(halfOfMessageLoadLimit)
        // we’ve consumed all the buffered messages, so a single request will fail
        .thenRequest(1)
        .expectError()
        .verify();
  }

  @Test
  void testDeleteMessageByDestinationAndGuid() throws Exception {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    final Device primary = DevicesHelper.createDevice((byte) 1);
    final Device device2 = DevicesHelper.createDevice((byte) 2);

    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, primary);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, primary);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, device2);

    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, device2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    final Optional<MessageProtos.Envelope> deletedMessage = messagesDynamoDb.deleteMessageByDestinationAndGuid(
        secondDestinationUuid, primary,
        UUID.fromString(MESSAGE2.getServerGuid())).get(5, TimeUnit.SECONDS);

    assertThat(deletedMessage).isPresent();

    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, device2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();

    final Optional<MessageProtos.Envelope> alreadyDeletedMessage = messagesDynamoDb.deleteMessageByDestinationAndGuid(
        secondDestinationUuid, primary,
        UUID.fromString(MESSAGE2.getServerGuid())).get(5, TimeUnit.SECONDS);

    assertThat(alreadyDeletedMessage).isNotPresent();

  }

  @Test
  void testDeleteSingleMessage() throws Exception {
    final UUID destinationUuid = UUID.randomUUID();
    final UUID secondDestinationUuid = UUID.randomUUID();
    final Device primary = DevicesHelper.createDevice((byte) 1);
    final Device device2 = DevicesHelper.createDevice((byte) 2);

    messagesDynamoDb.store(List.of(MESSAGE1), destinationUuid, primary);
    messagesDynamoDb.store(List.of(MESSAGE2), secondDestinationUuid, primary);
    messagesDynamoDb.store(List.of(MESSAGE3), destinationUuid, device2);

    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, device2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1).element(0).isEqualTo(MESSAGE2);

    messagesDynamoDb.deleteMessage(secondDestinationUuid, primary,
        UUID.fromString(MESSAGE2.getServerGuid()), MESSAGE2.getServerTimestamp()).get(1, TimeUnit.SECONDS);

    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull().hasSize(1)
        .element(0).isEqualTo(MESSAGE1);
    assertThat(load(destinationUuid, device2, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .hasSize(1)
        .element(0).isEqualTo(MESSAGE3);
    assertThat(load(secondDestinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE)).isNotNull()
        .isEmpty();
  }

  private List<MessageProtos.Envelope> load(final UUID destinationUuid, final Device destinationDevice,
      final int count) {
    return Flux.from(messagesDynamoDb.load(destinationUuid, destinationDevice, count))
        .take(count, true)
        .collectList()
        .block();
  }

  @Test
  void testLazyMessageDeletion() throws Exception {
    final UUID destinationUuid = UUID.randomUUID();
    final Device primary = DevicesHelper.createDevice((byte) 1);
    primary.setCreated(System.currentTimeMillis());

    messagesDynamoDb.store(List.of(MESSAGE1, MESSAGE2, MESSAGE3), destinationUuid, primary);
    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE))
        .as("load should return all messages stored").containsOnly(MESSAGE1, MESSAGE2, MESSAGE3);

    messagesDynamoDb.deleteMessageByDestinationAndGuid(destinationUuid, primary, UUID.fromString(MESSAGE1.getServerGuid()))
        .get(1, TimeUnit.SECONDS);
    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE))
        .as("deleting message by guid should work").containsExactly(MESSAGE3, MESSAGE2);

    messagesDynamoDb.deleteMessage(destinationUuid, primary, UUID.fromString(MESSAGE2.getServerGuid()), MESSAGE2.getServerTimestamp())
        .get(1, TimeUnit.SECONDS);
    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE))
        .as("deleting message by guid and timestamp should work").containsExactly(MESSAGE3);

    primary.setCreated(primary.getCreated() + 1000);
    assertThat(load(destinationUuid, primary, MessagesDynamoDb.RESULT_SET_CHUNK_SIZE))
        .as("devices with the same id but different create timestamps should see no messages")
        .isEmpty();
  }

  @Test
  void mayHaveMessages() {
    final UUID destinationUuid = UUID.randomUUID();
    final byte destinationDeviceId = (byte) (random.nextInt(Device.MAXIMUM_DEVICE_ID) + 1);
    final Device destinationDevice = DevicesHelper.createDevice(destinationDeviceId);

    assertThat(messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice).join()).isFalse();

    messagesDynamoDb.store(List.of(MESSAGE1, MESSAGE2, MESSAGE3), destinationUuid, destinationDevice);

    assertThat(messagesDynamoDb.mayHaveMessages(destinationUuid, destinationDevice).join()).isTrue();
  }

  @Test
  void mayHaveUrgentMessages() {
    final UUID destinationUuid = UUID.randomUUID();
    final byte destinationDeviceId = (byte) (random.nextInt(Device.MAXIMUM_DEVICE_ID) + 1);
    final Device destinationDevice = DevicesHelper.createDevice(destinationDeviceId);

    assertThat(messagesDynamoDb.mayHaveUrgentMessages(destinationUuid, destinationDevice).join()).isFalse();

    // used as the stable sort key, and the urgent message should be sorted last
    int serverTimestamp = 1;
    {
      final MessageProtos.Envelope nonUrgentMessage = MessageProtos.Envelope.newBuilder()
          .setUrgent(false)
          .setServerGuid(UUID.randomUUID().toString())
          .setDestinationServiceId(destinationUuid.toString())
          .setServerTimestamp(serverTimestamp++)
          .build();

      messagesDynamoDb.store(List.of(nonUrgentMessage), destinationUuid, destinationDevice);
    }

    assertThat(messagesDynamoDb.mayHaveUrgentMessages(destinationUuid, destinationDevice).join()).isFalse();

    {
      final List<MessageProtos.Envelope> messages = new ArrayList<>();
      // store more non-urgent messages
      for (int i = 0; i < MessagesDynamoDb.MAY_HAVE_URGENT_MESSAGES_QUERY_LIMIT * 5; i++) {
        messages.add(MessageProtos.Envelope.newBuilder()
            .setUrgent(false)
            .setServerGuid(UUID.randomUUID().toString())
            .setDestinationServiceId(destinationUuid.toString())
                .setServerTimestamp(serverTimestamp++)
            .build());
      }

      // and one urgent message
      messages.add(MessageProtos.Envelope.newBuilder()
          .setUrgent(true)
          .setServerGuid(UUID.randomUUID().toString())
          .setDestinationServiceId(destinationUuid.toString())
          .setServerTimestamp(serverTimestamp++)
          .build());

      messagesDynamoDb.store(messages, destinationUuid, destinationDevice);
    }

    assertThat(messagesDynamoDb.mayHaveUrgentMessages(destinationUuid, destinationDevice).join()).isTrue();
  }
}
