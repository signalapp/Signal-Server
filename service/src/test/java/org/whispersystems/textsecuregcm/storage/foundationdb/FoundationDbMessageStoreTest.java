package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.FoundationDbExtension;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FoundationDbMessageStoreTest {

  @RegisterExtension
  static FoundationDbExtension FOUNDATION_DB_EXTENSION = new FoundationDbExtension();

  private FoundationDbMessageStore foundationDbMessageStore;

  @BeforeEach
  void setup() {
    foundationDbMessageStore = new FoundationDbMessageStore(
        new Database[]{FOUNDATION_DB_EXTENSION.getDatabase()},
        Executors.newVirtualThreadPerTaskExecutor());
  }

  @Test
  void insert() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    final List<Byte> deviceIds = IntStream.range(Device.PRIMARY_ID, Device.PRIMARY_ID + 6)
        .mapToObj(i -> (byte) i)
        .toList();
    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = deviceIds.stream()
        .collect(Collectors.toMap(Function.identity(), _ -> generateRandomMessage()));
    final Versionstamp versionstamp = foundationDbMessageStore.insert(aci, messagesByDeviceId).join();
    assertNotNull(versionstamp);

    final Map<Byte, MessageProtos.Envelope> storedMessagesByDeviceId = deviceIds.stream()
        .collect(Collectors.toMap(Function.identity(), deviceId -> {
          try {
            return MessageProtos.Envelope.parseFrom(getMessageByVersionstamp(aci, deviceId, versionstamp));
          } catch (final InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
          }
        }));

    assertEquals(messagesByDeviceId, storedMessagesByDeviceId);
    assertEquals(versionstamp, getLastMessageVersionstamp(aci),
        "last message versionstamp should be the versionstamp of the last insert transaction");
  }

  @Test
  void versionstampCorrectlyUpdatedOnMultipleInserts() {
    final AciServiceIdentifier aci = new AciServiceIdentifier(UUID.randomUUID());
    foundationDbMessageStore.insert(aci, Map.of(Device.PRIMARY_ID, generateRandomMessage())).join();
    final Versionstamp secondMessageVersionstamp = foundationDbMessageStore.insert(aci,
        Map.of(Device.PRIMARY_ID, generateRandomMessage())).join();
    assertEquals(secondMessageVersionstamp, getLastMessageVersionstamp(aci));
  }

  private static MessageProtos.Envelope generateRandomMessage() {
    return MessageProtos.Envelope.newBuilder()
        .setContent(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
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

  private Versionstamp getLastMessageVersionstamp(final AciServiceIdentifier aci) {
    return FOUNDATION_DB_EXTENSION.getDatabase()
        .read(transaction -> transaction.get(foundationDbMessageStore.getLastMessageKey(aci))
            .thenApply(Tuple::fromBytes)
            .thenApply(t -> t.getVersionstamp(0)))
        .join();
  }

}
