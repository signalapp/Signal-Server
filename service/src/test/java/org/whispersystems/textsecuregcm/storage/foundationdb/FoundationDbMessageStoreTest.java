package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.util.DataSize;
import java.io.UncheckedIOException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.FoundationDbClusterExtension;
import org.whispersystems.textsecuregcm.storage.MessageStream;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.test.StepVerifier;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FoundationDbMessageStoreTest {

  @RegisterExtension
  static FoundationDbClusterExtension FOUNDATION_DB_EXTENSION = new FoundationDbClusterExtension(2);

  private VersionstampUUIDCipher versionstampUUIDCipher;
  private FoundationDbMessageStore foundationDbMessageStore;

  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(500), ZoneId.of("UTC"));

  @BeforeEach
  void setup() {
    final byte[] versionstampCipherKey = new byte[16];
    new SecureRandom().nextBytes(versionstampCipherKey);

    versionstampUUIDCipher = new VersionstampUUIDCipher(0, versionstampCipherKey);

    foundationDbMessageStore = new FoundationDbMessageStore(
        FOUNDATION_DB_EXTENSION.getDatabases(),
        versionstampUUIDCipher,
        Executors.newVirtualThreadPerTaskExecutor(),
        CLOCK);
  }

  @ParameterizedTest
  @MethodSource
  void insert(final long presenceUpdatedBeforeSeconds,
      final boolean ephemeral,
      final boolean expectMessagesInserted,
      final boolean expectVersionstampUpdated,
      final boolean expectPresenceState) {

    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    final List<Byte> deviceIds = IntStream.range(Device.PRIMARY_ID, Device.PRIMARY_ID + 6)
        .mapToObj(i -> (byte) i)
        .toList();

    deviceIds.forEach(deviceId -> writePresenceKey(aci, deviceId, 1, presenceUpdatedBeforeSeconds));

    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = deviceIds.stream()
        .collect(Collectors.toMap(Function.identity(), _ -> generateRandomMessage(ephemeral)));

    final Map<Byte, FoundationDbMessageStore.InsertResult> result =
        foundationDbMessageStore.insert(aci, messagesByDeviceId).join();

    assertTrue(result.keySet().containsAll(deviceIds));

    final Optional<Versionstamp> returnedVersionstamp = result.values().stream().findFirst()
        .flatMap(FoundationDbMessageStore.InsertResult::versionstamp);

    if (expectMessagesInserted) {
      assertTrue(returnedVersionstamp.isPresent());
      assertTrue(result.values().stream().allMatch(insertResult -> returnedVersionstamp.equals(insertResult.versionstamp())));

      final Map<Byte, MessageProtos.Envelope> storedMessagesByDeviceId = deviceIds.stream()
          .collect(Collectors.toMap(Function.identity(), deviceId -> {
            try {
              return MessageProtos.Envelope.parseFrom(
                  getMessageByVersionstamp(aci, deviceId, returnedVersionstamp.get()));
            } catch (final InvalidProtocolBufferException e) {
              throw new UncheckedIOException(e);
            }
          }));

      assertEquals(messagesByDeviceId, storedMessagesByDeviceId);
    } else {
      assertTrue(result.values().stream().allMatch(insertResult -> insertResult.versionstamp().isEmpty()));
    }

    if (expectVersionstampUpdated) {
      final Optional<Versionstamp> messagesAvailableWatchVersionstamp = getMessagesAvailableWatch(aci);
      assertTrue(messagesAvailableWatchVersionstamp.isPresent());
      assertEquals(messagesAvailableWatchVersionstamp, returnedVersionstamp,
          "messages available versionstamp should be the versionstamp of the last insert transaction");
    } else {
      assertTrue(getMessagesAvailableWatch(aci).isEmpty());
    }

    assertTrue(result.values().stream().allMatch(insertResult -> insertResult.present() == expectPresenceState));
  }

  private static Stream<Arguments> insert() {
    return Stream.of(
        Arguments.argumentSet("Non-ephemeral messages with all devices online",
            10L, false, true, true, true),
        Arguments.argumentSet(
            "Ephemeral messages with presence updated exactly at the second before which the device would be considered offline",
            300L, true, true, true, true),
        Arguments.argumentSet("Non-ephemeral messages for with all devices offline",
            310L, false, true, false, false),
        Arguments.argumentSet("Ephemeral messages with all devices offline",
            310L, true, false, false, false)
    );
  }

  @Test
  void versionstampCorrectlyUpdatedOnMultipleInserts() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    writePresenceKey(aci, Device.PRIMARY_ID, 1, 10L);
    foundationDbMessageStore.insert(Map.of(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage(false)))).join();
    final Map<Byte, FoundationDbMessageStore.InsertResult> secondMessageInsertResult = foundationDbMessageStore.insert(aci,
        Map.of(Device.PRIMARY_ID, generateRandomMessage(false))).join();

    final Optional<Versionstamp> messagesAvailableWatchVersionstamp = getMessagesAvailableWatch(aci);
    assertTrue(messagesAvailableWatchVersionstamp.isPresent());
    assertEquals(
        secondMessageInsertResult.get(Device.PRIMARY_ID).versionstamp(),
        messagesAvailableWatchVersionstamp);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void insertOnlyOneDevicePresent(final boolean ephemeral) {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    final List<Byte> deviceIds = IntStream.range(Device.PRIMARY_ID, Device.PRIMARY_ID + 6)
        .mapToObj(i -> (byte) i)
        .toList();
    // Only 1 device has a recent presence, the others do not have presence keys present.
    writePresenceKey(aci, Device.PRIMARY_ID, 1, 10L);
    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = deviceIds.stream()
        .collect(Collectors.toMap(Function.identity(), _ -> generateRandomMessage(ephemeral)));
    final Map<Byte, FoundationDbMessageStore.InsertResult> result = foundationDbMessageStore.insert(aci, messagesByDeviceId).join();
    assertNotNull(result);
    final Optional<Versionstamp> returnedVersionstamp = result.get(Device.PRIMARY_ID).versionstamp();
    assertTrue(returnedVersionstamp.isPresent(),
        "versionstamp should be present for online device");

    assertArrayEquals(
        messagesByDeviceId.get(Device.PRIMARY_ID).toByteArray(),
        getMessageByVersionstamp(aci, Device.PRIMARY_ID, returnedVersionstamp.get()),
        "Message for primary should always be stored since it has a recently updated presence");

    if (ephemeral) {
      assertTrue(IntStream.range(Device.PRIMARY_ID + 1, Device.PRIMARY_ID + 6)
          .mapToObj(deviceId -> getMessageByVersionstamp(aci, (byte) deviceId, returnedVersionstamp.get()))
          .allMatch(Objects::isNull), "Ephemeral messages for non-present devices must not be stored");
      assertTrue(IntStream.range(Device.PRIMARY_ID + 1, Device.PRIMARY_ID + 6)
              .mapToObj(deviceId -> result.get((byte) deviceId).versionstamp())
              .allMatch(Optional::isEmpty),
          "Unexpected versionstamp found for one or more devices that didn't have any messages inserted");
    } else {
      IntStream.range(Device.PRIMARY_ID + 1, Device.PRIMARY_ID)
          .forEach(deviceId -> {
            final byte[] messageBytes = getMessageByVersionstamp(aci, (byte) deviceId, returnedVersionstamp.get());
            assertEquals(messagesByDeviceId.get((byte) deviceId).toByteArray(), messageBytes,
                "Non-ephemeral messages must always be stored");
          });
    }

  }

  @ParameterizedTest
  @MethodSource
  void isClientPresent(final byte[] presenceValueBytes, final boolean expectPresent) {
    assertEquals(expectPresent, foundationDbMessageStore.isClientPresent(presenceValueBytes));
  }

  static Stream<Arguments> isClientPresent() {
    return Stream.of(
        Arguments.argumentSet("Presence value doesn't exist",
            null, false),
        Arguments.argumentSet("Presence updated recently",
            Conversions.longToByteArray(constructPresenceValue(42, getEpochSecondsBeforeClock(5))), true),
        Arguments.argumentSet("Presence updated same second as current time",
            Conversions.longToByteArray(constructPresenceValue(42, getEpochSecondsBeforeClock(0))), true),
        Arguments.argumentSet(
            "Presence updated exactly at the second before which it would have been considered offline",
            Conversions.longToByteArray(constructPresenceValue(42, getEpochSecondsBeforeClock(300))), true),
        Arguments.argumentSet("Presence expired",
            Conversions.longToByteArray(constructPresenceValue(42, getEpochSecondsBeforeClock(400))), false)
    );
  }

  /// Represents a cohort of recipients with the same config
  record MultiRecipientTestConfig(int shardNum, int numRecipients, boolean devicePresent,
                                  boolean generateEphemeralMessages, boolean expectMessagesInserted) {}

  @ParameterizedTest
  @MethodSource
  void insertMultiRecipient(final List<MultiRecipientTestConfig> testConfigs, final DataSize contentSize,
      final int[] expectedNumTransactionsByShard) {
    // Generate a list of ACIs for each test config
    final List<List<AciServiceIdentifier>> acisByConfig = testConfigs.stream()
        .map(testConfig -> IntStream.range(0, testConfig.numRecipients())
            .mapToObj(_ -> generateRandomAciForShard(testConfig.shardNum()))
            .toList())
        .toList();

    // Generate MRM bundles for each ACI, for each test config. Later, we'll assert if the stored messages (if expected)
    // are the same as those we generated.
    final List<Map<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>>> mrmByConfig = IntStream.range(0,
            testConfigs.size())
        .mapToObj(i -> {
          final List<AciServiceIdentifier> acis = acisByConfig.get(i);
          final MultiRecipientTestConfig testConfig = testConfigs.get(i);
          return acis.stream()
              .collect(Collectors.toMap(
                  Function.identity(),
                  _ -> Map.of(Device.PRIMARY_ID,
                      generateRandomMessage(testConfig.generateEphemeralMessages(), (int) contentSize.toBytes()))));

        })
        .toList();

    // Create the consolidated MRM bundle by ACI.
    final Map<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>> mrmBundle = new HashMap<>();
    mrmByConfig.forEach(mrmBundle::putAll);

    // Write a presence key for the cohort of recipients if the config indicates that the device must be present.
    for (int i = 0; i < testConfigs.size(); i++) {
      final List<AciServiceIdentifier> acis = acisByConfig.get(i);
      final MultiRecipientTestConfig testConfig = testConfigs.get(i);
      if (testConfig.devicePresent()) {
        acis.forEach(aci -> writePresenceKey(aci, Device.PRIMARY_ID, 1, 10L));
      }
    }

    final Map<AciServiceIdentifier, Map<Byte, FoundationDbMessageStore.InsertResult>> result = foundationDbMessageStore.insert(mrmBundle).join();
    assertNotNull(result);

    // Compute the set of versionstamps by shard number from the individual device insert results, so that we can
    // assert that each shard has the expected number of committed transactions.
    final Map<Integer, Set<Versionstamp>> returnedVersionstampsByShard = new HashMap<>();
    result.forEach((aci, deviceResults) -> {
      final int shardNum = foundationDbMessageStore.hashAciToShardNumber(aci);
      final Set<Versionstamp> versionstampSet = returnedVersionstampsByShard.computeIfAbsent(shardNum, _ -> new HashSet<>());
      deviceResults.forEach((_, deviceResult) -> deviceResult.versionstamp().ifPresent(versionstampSet::add));
    });

    final int[] returnedNumVersionstampsByShard = new int[FOUNDATION_DB_EXTENSION.getDatabases().length];
    for (int i = 0; i < returnedNumVersionstampsByShard.length; i++) {
      returnedNumVersionstampsByShard[i] = returnedVersionstampsByShard.getOrDefault(i, Collections.emptySet()).size();
    }

    assertArrayEquals(expectedNumTransactionsByShard, returnedNumVersionstampsByShard);

    // For each cohort of recipients, check whether the stored messages (if expected) are the same as those we inserted
    // and whether the returned device presence states are the same as the configured states.
    IntStream.range(0, testConfigs.size()).forEach(i -> {
      final List<AciServiceIdentifier> acis = acisByConfig.get(i);
      final MultiRecipientTestConfig shardConfig = testConfigs.get(i);
      if (shardConfig.expectMessagesInserted()) {
        final Map<AciServiceIdentifier, Map<Byte, MessageProtos.Envelope>> storedMrmBundle = acis.stream()
            .collect(Collectors.toMap(Function.identity(), aci -> {
              final List<KeyValue> items = getItemsInDeviceQueue(aci, Device.PRIMARY_ID);
              assertEquals(1, items.size());
              try {
                final MessageProtos.Envelope envelope = MessageProtos.Envelope.parseFrom(items.getFirst().getValue());
                return Map.of(Device.PRIMARY_ID, envelope);
              } catch (final InvalidProtocolBufferException e) {
                throw new UncheckedIOException(e);
              }
            }));
        assertEquals(mrmByConfig.get(i), storedMrmBundle,
            "Stored message bundle does not match inserted message bundle");
      } else {
        assertEquals(0, acis
            .stream()
            .mapToInt(aci -> getItemsInDeviceQueue(aci, Device.PRIMARY_ID).size())
            .sum(), "Unexpected messages found in device queue");
      }

      assertTrue(acis
              .stream()
              .allMatch(
                  aci -> result.get(aci).get(Device.PRIMARY_ID).present() == shardConfig.devicePresent()),
          "Device presence state from insert result does not match expected state");
    });
  }

  static Stream<Arguments> insertMultiRecipient() {
    return Stream.of(
        Arguments.argumentSet("Multiple recipients on a single shard should result in a single transaction",
            List.of(
                new MultiRecipientTestConfig(0, 5, true, false, true)),
            DataSize.bytes(128), new int[] {1, 0}),
        Arguments.argumentSet(
            "Multiple recipients on a single shard exceeding the transaction limit should be broken up into multiple transactions",
            List.of(
                new MultiRecipientTestConfig(0, 15, true, false, true)),
            DataSize.kilobytes(90), new int[] {2, 0}),
        Arguments.argumentSet("Multiple recipients on different shards should result in multiple transactions",
            List.of(
                new MultiRecipientTestConfig(0, 5, true, false, true),
                new MultiRecipientTestConfig(1, 5, true, false, true)),
            DataSize.bytes(128), new int[] {1, 1}),
        Arguments.argumentSet(
            "Multiple recipients on different shards each exceeding the transaction limit should be broken up into multiple transactions on each shard",
            List.of(
                new MultiRecipientTestConfig(0, 15, true, false, true),
                new MultiRecipientTestConfig(1, 15, true, false, true)),
           DataSize.kilobytes(90), new int[] {2, 2}),
        Arguments.argumentSet(
            "Multiple recipients on a single shard with ephemeral messages and no devices present should result in no transactions committed",
            List.of(
                new MultiRecipientTestConfig(0, 5, false, true, false)),
            DataSize.bytes(128), new int[] {0, 0}),
        Arguments.argumentSet(
            "Multiple recipients on different shards with ephemeral messages and no devices present should result in no transactions committed",
            List.of(
                new MultiRecipientTestConfig(0, 5, false, true, false),
                new MultiRecipientTestConfig(1, 5, false, true, false)),
            DataSize.bytes(128), new int[] {0, 0}),
        Arguments.argumentSet(
            "Multiple recipients on two shards with one shard having no devices present should result in only one transaction",
            List.of(
                new MultiRecipientTestConfig(0, 5, false, true, false),
                new MultiRecipientTestConfig(1, 5, true, true, true)),
            DataSize.bytes(128), new int[] {0, 1}),
        Arguments.argumentSet(
            "Multiple recipients on a single shard with some recipients having no devices present should result in only one transaction",
            List.of(
                new MultiRecipientTestConfig(0, 3, false, true, false),
                new MultiRecipientTestConfig(0, 3, true, true, true)),
            DataSize.bytes(128), new int[] {1, 0}),
        Arguments.argumentSet(
            "Multiple recipients on a single shard with total size just exceeding 2 chunks should result in 3 transactions",
            List.of(
                new MultiRecipientTestConfig(0, 23, true, false, true)),
            DataSize.kilobytes(90), new int[] {3, 0})
    );
  }

  @Test
  void insertEmptyBundle() {
    assertThrows(IllegalArgumentException.class, () -> foundationDbMessageStore.insert(
        Map.of(generateRandomAciForShard(0), Collections.emptyMap())));
  }

  @ParameterizedTest
  @MethodSource
  void getMessages(final int numMessages, final int batchSize) {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    writePresenceKey(aci, Device.PRIMARY_ID, 1, 5L);

    final List<Versionstamp> expectedVersionstamps = IntStream.range(0, numMessages)
        .mapToObj(_ -> foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage(false))).join()
            .get(Device.PRIMARY_ID)
            .versionstamp()
            .orElseThrow())
        .toList();

    final Device device = new Device();
    device.setId(Device.PRIMARY_ID);
    final MessageStream messageStream = foundationDbMessageStore.getMessages(aci, device, batchSize);
    final List<MessageStreamEntry> retrievedEntries = new ArrayList<>();
    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messageStream.getMessages()))
        .recordWith(() -> retrievedEntries)
        .expectNextCount(numMessages)
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .verifyTimeout(Duration.ofSeconds(1));

    final MessageGuidCodec messageGuidCodec =
        new MessageGuidCodec(aci.uuid(), Device.PRIMARY_ID, versionstampUUIDCipher);

    for (int i = 0; i < expectedVersionstamps.size(); i++) {
      final MessageStreamEntry.Envelope envelopeEntry =
          assertInstanceOf(MessageStreamEntry.Envelope.class, retrievedEntries.get(i));

      assertEquals(expectedVersionstamps.get(i),
          messageGuidCodec.decodeMessageGuid(UUIDUtil.fromByteString(envelopeEntry.message().getServerGuidBinary())));
    }
  }

  static Stream<Arguments> getMessages() {
    final int batchSize = 16;
    
    return Stream.of(
        Arguments.argumentSet("Single message", 1, batchSize),
        Arguments.argumentSet("Queue is empty", 0, batchSize),
        Arguments.argumentSet("Multiple of batch size", batchSize * 4, batchSize),
        Arguments.argumentSet("Non-multiple of batch size", (batchSize * 4) + 1, batchSize)
    );
  }

  @Test
  void getMessagesPublishMoreAfterQueueEmpty() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    final MessageGuidCodec messageGuidCodec =
        new MessageGuidCodec(aci.uuid(), Device.PRIMARY_ID, versionstampUUIDCipher);

    writePresenceKey(aci, Device.PRIMARY_ID, 1, 5L);
    final MessageProtos.Envelope message1 = generateRandomMessage(false);
    final Versionstamp versionstamp1 = foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, message1)).join()
        .get(Device.PRIMARY_ID)
        .versionstamp()
        .orElseThrow();

    final MessageProtos.Envelope message2 = generateRandomMessage(false);
    final Versionstamp versionstamp2 = foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, message2)).join()
        .get(Device.PRIMARY_ID)
        .versionstamp()
        .orElseThrow();

    final CountDownLatch latch = new CountDownLatch(1);
    final MessageProtos.Envelope message3 = generateRandomMessage(false);
    final AtomicReference<Versionstamp> versionstamp3 = new AtomicReference<>();
    Thread.ofVirtual().start(() -> {
      try {
        // Wait until queue is empty
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        // Then publish more messages
        synchronized (versionstamp3) {
          versionstamp3.set(foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, message3)).join()
              .get(Device.PRIMARY_ID)
              .versionstamp()
              .orElseThrow());

          versionstamp3.notifyAll();
        }
      } catch (final InterruptedException e) {
        fail(e);
      }
    });

    final Device device = new Device();
    device.setId(Device.PRIMARY_ID);
    final MessageStream messageStream = foundationDbMessageStore.getMessages(aci, device);
    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messageStream.getMessages()))
        .expectNext(new MessageStreamEntry.Envelope(message1
            .toBuilder()
            .setServerGuidBinary(UUIDUtil.toByteString(messageGuidCodec.encodeMessageGuid(versionstamp1)))
            .build()))
        .expectNext(new MessageStreamEntry.Envelope(message2
            .toBuilder()
            .setServerGuidBinary(UUIDUtil.toByteString(messageGuidCodec.encodeMessageGuid(versionstamp2)))
            .build()))
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .then(() -> {
          // Trigger insertion of another message
          latch.countDown();

          // …but then wait for its versionstamp so we can verify that we have the right payload
          synchronized (versionstamp3) {
            while (versionstamp3.get() == null) {
              try {
                versionstamp3.wait();
              } catch (final InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          }
        })
        .expectNextMatches(entry -> entry.equals(new MessageStreamEntry.Envelope(message3
            .toBuilder()
            .setServerGuidBinary(UUIDUtil.toByteString(messageGuidCodec.encodeMessageGuid(versionstamp3.get())))
            .build())))
        .verifyTimeout(Duration.ofSeconds(3));
  }

  @Test
  void getMessagesPublishMoreAfterSubscriptionStarts() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    writePresenceKey(aci, Device.PRIMARY_ID, 1, 5L);
    for (int i = 0; i < 16; i++) {
      final MessageProtos.Envelope message = generateRandomMessage(false);
      assertNotNull(foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, message)).join());
    }

    final CountDownLatch latch = new CountDownLatch(1);
    Thread.ofVirtual().start(() -> {
      try {
        // Wait until queue is empty
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        // Then publish more messages
        for (int i = 0; i < 16; i++) {
          final MessageProtos.Envelope message = generateRandomMessage(false);
          assertNotNull(foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, message)).join());
        }
      } catch (final InterruptedException e) {
        fail(e);
      }
    });

    final Device device = new Device();
    device.setId(Device.PRIMARY_ID);
    final MessageStream messageStream = foundationDbMessageStore.getMessages(aci, device);
    // Initially only request a single message, then give the go ahead to the publisher. This verifies that we get the
    // queue empty signal when we consume the initial batch of messages even though the publisher keeps publishing in
    // the meantime.
    StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(messageStream.getMessages()), 1)
        .expectNextCount(1)
        .then(latch::countDown)
        .thenRequest(100)
        .expectNextCount(15)
        .expectNext(new MessageStreamEntry.QueueEmpty())
        .expectNextCount(16)
        .verifyTimeout(Duration.ofSeconds(3));
  }

  static MessageProtos.Envelope generateRandomMessage(final boolean ephemeral) {
    return generateRandomMessage(ephemeral, 16);
  }

  static MessageProtos.Envelope generateRandomMessage(final boolean ephemeral, final int contentSize) {
    return MessageProtos.Envelope.newBuilder()
        .setContent(ByteString.copyFrom(TestRandomUtil.nextBytes(contentSize)))
        .setEphemeral(ephemeral)
        .build();
  }

  private byte[] getMessageByVersionstamp(final AciServiceIdentifier aci, final byte deviceId,
      final Versionstamp versionstamp) {
    return foundationDbMessageStore.getShardForAci(aci).read(transaction -> {
      final byte[] key = foundationDbMessageStore.getDeviceQueueSubspace(aci, deviceId)
          .pack(Tuple.from(versionstamp));
      return transaction.get(key);
    }).join();
  }

  private Optional<Versionstamp> getMessagesAvailableWatch(final AciServiceIdentifier aci) {
    return foundationDbMessageStore.getShardForAci(aci)
        .read(transaction -> transaction.get(foundationDbMessageStore.getMessagesAvailableWatchKey(aci))
            .thenApply(value -> value == null ? null : Tuple.fromBytes(value).getVersionstamp(0))
            .thenApply(Optional::ofNullable))
        .join();
  }

  private void writePresenceKey(final AciServiceIdentifier aci, final byte deviceId, final int serverId,
      final long secondsBeforeCurrentTime) {
    foundationDbMessageStore.getShardForAci(aci).run(transaction -> {
      final byte[] presenceKey = foundationDbMessageStore.getPresenceKey(aci, deviceId);
      final long presenceUpdateEpochSeconds = getEpochSecondsBeforeClock(secondsBeforeCurrentTime);
      final long presenceValue = constructPresenceValue(serverId, presenceUpdateEpochSeconds);
      transaction.set(presenceKey, Conversions.longToByteArray(presenceValue));
      return null;
    });
  }

  private static long getEpochSecondsBeforeClock(final long secondsBefore) {
    return CLOCK.instant().minusSeconds(secondsBefore).getEpochSecond();
  }

  private static long constructPresenceValue(final int serverId, final long presenceUpdateEpochSeconds) {
    return (long) (serverId & 0x0ffff) << 48 | (presenceUpdateEpochSeconds & 0x0000ffffffffffffL);
  }

  private AciServiceIdentifier generateRandomAciForShard(final int shardNumber) {
    assert shardNumber < FOUNDATION_DB_EXTENSION.getDatabases().length;
    while (true) {
      final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
      if (foundationDbMessageStore.hashAciToShardNumber(aci) == shardNumber) {
        return aci;
      }
    }
  }

  private List<KeyValue> getItemsInDeviceQueue(final AciServiceIdentifier aci, final byte deviceId) {
    return foundationDbMessageStore.getShardForAci(aci).readAsync(transaction -> AsyncUtil.collect(transaction.getRange(
        foundationDbMessageStore.getDeviceQueueSubspace(aci, deviceId).range()))).join();
  }

}
