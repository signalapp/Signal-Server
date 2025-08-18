package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
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
import org.whispersystems.textsecuregcm.storage.FoundationDbExtension;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FoundationDbMessageStoreTest {

  @RegisterExtension
  static FoundationDbExtension FOUNDATION_DB_EXTENSION = new FoundationDbExtension();

  private FoundationDbMessageStore foundationDbMessageStore;

  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(500), ZoneId.of("UTC"));

  @BeforeEach
  void setup() {
    foundationDbMessageStore = new FoundationDbMessageStore(
        new Database[]{FOUNDATION_DB_EXTENSION.getDatabase()},
        Executors.newVirtualThreadPerTaskExecutor(),
        CLOCK);
  }

  @ParameterizedTest
  @MethodSource
  void insert(final long presenceUpdatedBeforeSeconds, final boolean ephemeral, final boolean expectMessagesInserted,
      final boolean expectVersionstampUpdated) {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    final List<Byte> deviceIds = IntStream.range(Device.PRIMARY_ID, Device.PRIMARY_ID + 6)
        .mapToObj(i -> (byte) i)
        .toList();
    deviceIds.forEach(deviceId -> writePresenceKey(aci, deviceId, 1, presenceUpdatedBeforeSeconds));
    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = deviceIds.stream()
        .collect(Collectors.toMap(Function.identity(), _ -> generateRandomMessage(ephemeral)));
    final Optional<Versionstamp> versionstamp = foundationDbMessageStore.insert(aci, messagesByDeviceId).join();
    assertNotNull(versionstamp);

    if (expectMessagesInserted) {
      assertTrue(versionstamp.isPresent());
      final Map<Byte, MessageProtos.Envelope> storedMessagesByDeviceId = deviceIds.stream()
          .collect(Collectors.toMap(Function.identity(), deviceId -> {
            try {
              return MessageProtos.Envelope.parseFrom(getMessageByVersionstamp(aci, deviceId, versionstamp.get()));
            } catch (final InvalidProtocolBufferException e) {
              throw new UncheckedIOException(e);
            }
          }));

      assertEquals(messagesByDeviceId, storedMessagesByDeviceId);
    } else {
      assertTrue(versionstamp.isEmpty());
    }

    if (expectVersionstampUpdated) {
      assertEquals(versionstamp, getMessagesAvailableWatch(aci),
          "messages available versionstamp should be the versionstamp of the last insert transaction");
    } else {
      assertTrue(getMessagesAvailableWatch(aci).isEmpty());
    }
  }

  private static Stream<Arguments> insert() {
    return Stream.of(
        Arguments.argumentSet("Non-ephemeral messages with all devices online",
            10L, false, true, true),
        Arguments.argumentSet(
            "Ephemeral messages with presence updated exactly at the second before which the device would be considered offline",
            300L, true, true, true),
        Arguments.argumentSet("Non-ephemeral messages for with all devices offline",
            310L, false, true, false),
        Arguments.argumentSet("Ephemeral messages with all devices offline",
            310L, true, false, false)
    );
  }

  @Test
  void versionstampCorrectlyUpdatedOnMultipleInserts() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    writePresenceKey(aci, Device.PRIMARY_ID, 1, 10L);
    foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage(false))).join();
    final Optional<Versionstamp> secondMessageVersionstamp = foundationDbMessageStore.insert(aci,
        Map.of(Device.PRIMARY_ID, generateRandomMessage(false))).join();
    assertEquals(secondMessageVersionstamp, getMessagesAvailableWatch(aci));
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
    final Optional<Versionstamp> versionstamp = foundationDbMessageStore.insert(aci, messagesByDeviceId).join();
    assertNotNull(versionstamp);
    assertTrue(versionstamp.isPresent(),
        "versionstamp should be present since at least one message should be inserted");

    assertArrayEquals(
        messagesByDeviceId.get(Device.PRIMARY_ID).toByteArray(),
        getMessageByVersionstamp(aci, Device.PRIMARY_ID, versionstamp.get()),
        "Message for primary should always be stored since it has a recently updated presence");

    if (ephemeral) {
      assertTrue(IntStream.range(Device.PRIMARY_ID + 1, Device.PRIMARY_ID + 6)
          .mapToObj(deviceId -> getMessageByVersionstamp(aci, (byte) deviceId, versionstamp.get()))
          .allMatch(Objects::isNull), "Ephemeral messages for non-present devices must not be stored");
    } else {
      IntStream.range(Device.PRIMARY_ID + 1, Device.PRIMARY_ID)
          .forEach(deviceId -> {
            final byte[] messageBytes = getMessageByVersionstamp(aci, (byte) deviceId, versionstamp.get());
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
        Arguments.argumentSet("Presence updated exactly at the second before which it would have been considered offline",
            Conversions.longToByteArray(constructPresenceValue(42, getEpochSecondsBeforeClock(300))), true),
        Arguments.argumentSet("Presence expired",
            Conversions.longToByteArray(constructPresenceValue(42, getEpochSecondsBeforeClock(400))), false)
    );
  }

  private static MessageProtos.Envelope generateRandomMessage(final boolean ephemeral) {
    return MessageProtos.Envelope.newBuilder()
        .setContent(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
        .setEphemeral(ephemeral)
        .build();
  }

  private byte[] getMessageByVersionstamp(final AciServiceIdentifier aci, final byte deviceId,
      final Versionstamp versionstamp) {
    return FOUNDATION_DB_EXTENSION.getDatabase().read(transaction -> {
      final byte[] key = foundationDbMessageStore.getDeviceQueueSubspace(aci, deviceId)
          .pack(Tuple.from(versionstamp));
      return transaction.get(key);
    }).join();
  }

  private Optional<Versionstamp> getMessagesAvailableWatch(final AciServiceIdentifier aci) {
    return FOUNDATION_DB_EXTENSION.getDatabase()
        .read(transaction -> transaction.get(foundationDbMessageStore.getMessagesAvailableWatchKey(aci))
            .thenApply(value -> value == null ? null : Tuple.fromBytes(value).getVersionstamp(0))
            .thenApply(Optional::ofNullable))
        .join();
  }

  private void writePresenceKey(final AciServiceIdentifier aci, final byte deviceId, final int serverId,
      final long secondsBeforeCurrentTime) {
    FOUNDATION_DB_EXTENSION.getDatabase().run(transaction -> {
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

}
