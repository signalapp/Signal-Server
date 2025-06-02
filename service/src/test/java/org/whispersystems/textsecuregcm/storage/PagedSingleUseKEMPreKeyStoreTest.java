/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

class PagedSingleUseKEMPreKeyStoreTest {

  private static final int KEY_COUNT = 100;
  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private static final String BUCKET_NAME = "testbucket";

  private PagedSingleUseKEMPreKeyStore keyStore;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS);

  @RegisterExtension
  static final S3LocalStackExtension S3_EXTENSION = new S3LocalStackExtension(BUCKET_NAME);

  @BeforeEach
  void setUp() {
    keyStore = new PagedSingleUseKEMPreKeyStore(
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        S3_EXTENSION.getS3Client(),
        DynamoDbExtensionSchema.Tables.PAGED_PQ_KEYS.tableName(),
        BUCKET_NAME);
  }

  @Test
  void storeTake() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(Optional.empty(), keyStore.take(accountIdentifier, deviceId).join());

    final List<KEMSignedPreKey> preKeys = generateRandomPreKeys();
    assertDoesNotThrow(() -> keyStore.store(accountIdentifier, deviceId, preKeys).join());

    final List<KEMSignedPreKey> sortedPreKeys = preKeys.stream()
        .sorted(Comparator.comparing(KEMSignedPreKey::keyId))
        .toList();

    assertEquals(Optional.of(sortedPreKeys.get(0)), keyStore.take(accountIdentifier, deviceId).join());
    assertEquals(Optional.of(sortedPreKeys.get(1)), keyStore.take(accountIdentifier, deviceId).join());
  }

  @Test
  void storeTwice() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    final List<KEMSignedPreKey> preKeys1 = generateRandomPreKeys();
    keyStore.store(accountIdentifier, deviceId, preKeys1).join();
    List<String> oldPages = listPages(accountIdentifier).stream().map(S3Object::key).toList();
    assertEquals(1, oldPages.size());

    final List<KEMSignedPreKey> preKeys2 = generateRandomPreKeys();
    keyStore.store(accountIdentifier, deviceId, preKeys2).join();
    List<String> newPages = listPages(accountIdentifier).stream().map(S3Object::key).toList();
    assertEquals(1, newPages.size());

    assertNotEquals(oldPages.getFirst(), newPages.getFirst());

    assertEquals(
        preKeys2.stream().sorted(Comparator.comparing(KEMSignedPreKey::keyId)).toList(),

        IntStream.range(0, preKeys2.size())
            .mapToObj(i -> keyStore.take(accountIdentifier, deviceId).join())
            .map(Optional::orElseThrow)
            .toList());

    assertTrue(keyStore.take(accountIdentifier, deviceId).join().isEmpty());

  }

  @Test
  void takeAll() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    final List<KEMSignedPreKey> preKeys = generateRandomPreKeys();
    assertDoesNotThrow(() -> keyStore.store(accountIdentifier, deviceId, preKeys).join());

    final List<KEMSignedPreKey> sortedPreKeys = preKeys.stream()
        .sorted(Comparator.comparing(KEMSignedPreKey::keyId))
        .toList();

    for (int i = 0; i < KEY_COUNT; i++) {
      assertEquals(Optional.of(sortedPreKeys.get(i)), keyStore.take(accountIdentifier, deviceId).join());
    }
    assertEquals(0, keyStore.getCount(accountIdentifier, deviceId).join());
    assertTrue(keyStore.take(accountIdentifier, deviceId).join().isEmpty());
  }

  @Test
  void getCount() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, keyStore.getCount(accountIdentifier, deviceId).join());

    final List<KEMSignedPreKey> preKeys = generateRandomPreKeys();

    keyStore.store(accountIdentifier, deviceId, preKeys).join();

    assertEquals(KEY_COUNT, keyStore.getCount(accountIdentifier, deviceId).join());

    for (int i = 0; i < KEY_COUNT; i++) {
      keyStore.take(accountIdentifier, deviceId).join();
      assertEquals(KEY_COUNT - (i + 1), keyStore.getCount(accountIdentifier, deviceId).join());
    }
  }

  @Test
  void deleteSingleDevice() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, keyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> keyStore.delete(accountIdentifier, deviceId).join());

    final List<KEMSignedPreKey> preKeys = generateRandomPreKeys();

    keyStore.store(accountIdentifier, deviceId, preKeys).join();
    keyStore.store(accountIdentifier, (byte) (deviceId + 1), preKeys).join();

    assertDoesNotThrow(() -> keyStore.delete(accountIdentifier, deviceId).join());

    assertEquals(0, keyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(KEY_COUNT, keyStore.getCount(accountIdentifier, (byte) (deviceId + 1)).join());

    final List<S3Object> pages = listPages(accountIdentifier);
    assertEquals(1, pages.size());
    assertTrue(pages.getFirst().key().startsWith("%s/%s".formatted(accountIdentifier, deviceId + 1)));
  }

  @Test
  void deleteAllDevices() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = 1;

    assertEquals(0, keyStore.getCount(accountIdentifier, deviceId).join());
    assertDoesNotThrow(() -> keyStore.delete(accountIdentifier).join());

    final List<KEMSignedPreKey> preKeys = generateRandomPreKeys();

    keyStore.store(accountIdentifier, deviceId, preKeys).join();
    keyStore.store(accountIdentifier, (byte) (deviceId + 1), preKeys).join();

    assertDoesNotThrow(() -> keyStore.delete(accountIdentifier).join());

    assertEquals(0, keyStore.getCount(accountIdentifier, deviceId).join());
    assertEquals(0, keyStore.getCount(accountIdentifier, (byte) (deviceId + 1)).join());
    assertEquals(0, listPages(accountIdentifier).size());
  }

  @Test
  void listPages() {
    final UUID aci1 = UUID.randomUUID();
    final UUID aci2 = new UUID(aci1.getMostSignificantBits(), aci1.getLeastSignificantBits() + 1);
    final byte deviceId = 1;

    keyStore.store(aci1, deviceId, generateRandomPreKeys()).join();
    keyStore.store(aci1, (byte) (deviceId + 1), generateRandomPreKeys()).join();
    keyStore.store(aci2, deviceId, generateRandomPreKeys()).join();

    List<DeviceKEMPreKeyPages> stored = keyStore.listStoredPages(1).collectList().block();
    assertEquals(3, stored.size());
    for (DeviceKEMPreKeyPages pages : stored) {
      assertEquals(1, pages.pageIdToLastModified().size());
    }

    assertEquals(List.of(aci1, aci1, aci2), stored.stream().map(DeviceKEMPreKeyPages::identifier).toList());
    assertEquals(
        List.of(deviceId, (byte) (deviceId + 1), deviceId),
        stored.stream().map(DeviceKEMPreKeyPages::deviceId).toList());
  }

  @Test
  void listPagesWithOrphans() {
    final UUID aci1 = UUID.randomUUID();
    final UUID aci2 = new UUID(aci1.getMostSignificantBits(), aci1.getLeastSignificantBits() + 1);
    final byte deviceId = 1;

    // Two orphans
    keyStore.store(aci1, deviceId, generateRandomPreKeys()).join();
    writeOrphanedS3Object(aci1, deviceId);
    writeOrphanedS3Object(aci1, deviceId);

    // No orphans
    keyStore.store(aci1, (byte) (deviceId + 1), generateRandomPreKeys()).join();

    // One orphan
    keyStore.store(aci2, deviceId, generateRandomPreKeys()).join();
    writeOrphanedS3Object(aci2, deviceId);

    // Orphan with no database record
    writeOrphanedS3Object(aci2, (byte) (deviceId + 2));

    List<DeviceKEMPreKeyPages> stored = keyStore.listStoredPages(1).collectList().block();
    assertEquals(4, stored.size());
    
    assertEquals(
        List.of(3, 1, 2, 1),
        stored.stream().map(s -> s.pageIdToLastModified().size()).toList());
  }

  private void writeOrphanedS3Object(final UUID identifier, final byte deviceId) {
    S3_EXTENSION.getS3Client()
        .putObject(PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("%s/%s/%s".formatted(identifier, deviceId, UUID.randomUUID())).build(),
            AsyncRequestBody.fromBytes(TestRandomUtil.nextBytes(10)))
        .join();
  }

  private List<S3Object> listPages(final UUID identifier) {
    return Flux.from(S3_EXTENSION.getS3Client().listObjectsV2Paginator(ListObjectsV2Request.builder()
            .bucket(BUCKET_NAME)
            .prefix(identifier.toString())
            .build()))
        .concatMap(response -> Flux.fromIterable(response.contents()))
        .collectList()
        .block();
  }

  private List<KEMSignedPreKey> generateRandomPreKeys() {
    final Set<Integer> keyIds = new HashSet<>(KEY_COUNT);

    while (keyIds.size() < KEY_COUNT) {
      keyIds.add(Math.abs(ThreadLocalRandom.current().nextInt()));
    }

    return keyIds.stream()
        .map(keyId -> KeysHelper.signedKEMPreKey(keyId, IDENTITY_KEY_PAIR))
        .toList();
  }
}
