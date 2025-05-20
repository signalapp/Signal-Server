/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
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
        .sorted(Comparator.comparing(preKey -> preKey.keyId()))
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
    List<String> oldPages = listPages(accountIdentifier).stream().map(S3Object::key).collect(Collectors.toList());
    assertEquals(1, oldPages.size());

    final List<KEMSignedPreKey> preKeys2 = generateRandomPreKeys();
    keyStore.store(accountIdentifier, deviceId, preKeys2).join();
    List<String> newPages = listPages(accountIdentifier).stream().map(S3Object::key).collect(Collectors.toList());
    assertEquals(1, newPages.size());

    assertNotEquals(oldPages.getFirst(), newPages.getFirst());

    assertEquals(
        preKeys2.stream().sorted(Comparator.comparing(preKey -> preKey.keyId())).toList(),

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
        .sorted(Comparator.comparing(preKey -> preKey.keyId()))
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
    assertTrue(pages.get(0).key().startsWith("%s/%s".formatted(accountIdentifier, deviceId + 1)));
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
