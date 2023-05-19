/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

import static org.junit.jupiter.api.Assertions.*;

class KeysTest {

  private Keys keys;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.EC_KEYS, Tables.PQ_KEYS, Tables.PQ_LAST_RESORT_KEYS);

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final long DEVICE_ID = 1L;

  @BeforeEach
  void setup() {
    keys = new Keys(
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        Tables.EC_KEYS.tableName(),
        Tables.PQ_KEYS.tableName(),
        Tables.PQ_LAST_RESORT_KEYS.tableName());
  }

  @Test
  void testStore() {
    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Initial pre-key count for an account should be zero");
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Initial pre-key count for an account should be zero");
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent(),
        "Initial last-resort pre-key for an account should be missing");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1)));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1)));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Repeatedly storing same key should have no effect");

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(generateTestSignedPreKey(1)), null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new PQ prekeys should have no effect on EC prekeys");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, null, generateTestSignedPreKey(1001));
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new PQ last-resort prekey should have no effect on EC prekeys");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new PQ last-resort prekey should have no effect on one-time PQ prekeys");
    assertEquals(1001, keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).get().getKeyId());

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(2)), null, null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Uploading new EC prekeys should have no effect on PQ prekeys");

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(3)), List.of(generateTestSignedPreKey(2)), null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting a new key should overwrite all prior keys of the same type for the given account/device");

    keys.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(generateTestPreKey(4), generateTestPreKey(5)),
        List.of(generateTestSignedPreKey(6), generateTestSignedPreKey(7)),
        generateTestSignedPreKey(1002));
    assertEquals(2, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
    assertEquals(2, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID),
        "Inserting multiple new keys should overwrite all prior keys for the given account/device");
    assertEquals(1002, keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).get().getKeyId(),
        "Uploading new last-resort key should overwrite prior last-resort key for the account/device");
  }

  @Test
  void testTakeAccountAndDeviceId() {
    assertEquals(Optional.empty(), keys.takeEC(ACCOUNT_UUID, DEVICE_ID));

    final PreKey preKey = generateTestPreKey(1);

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(preKey, generateTestPreKey(2)));
    final Optional<PreKey> takenKey = keys.takeEC(ACCOUNT_UUID, DEVICE_ID);
    assertEquals(Optional.of(preKey), takenKey);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testTakePQ() {
    assertEquals(Optional.empty(), keys.takeEC(ACCOUNT_UUID, DEVICE_ID));

    final SignedPreKey preKey1 = generateTestSignedPreKey(1);
    final SignedPreKey preKey2 = generateTestSignedPreKey(2);
    final SignedPreKey preKeyLast = generateTestSignedPreKey(1001);

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(preKey1, preKey2), preKeyLast);

    assertEquals(Optional.of(preKey1), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    assertEquals(Optional.of(preKey2), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    assertEquals(Optional.of(preKeyLast), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    assertEquals(Optional.of(preKeyLast), keys.takePQ(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testGetCount() {
    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));

    keys.store(ACCOUNT_UUID, DEVICE_ID, List.of(generateTestPreKey(1)), List.of(generateTestSignedPreKey(1)), null);
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
  }

  @Test
  void testDeleteByAccount() {
    keys.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(generateTestPreKey(1), generateTestPreKey(2)),
        List.of(generateTestSignedPreKey(3), generateTestSignedPreKey(4)),
        generateTestSignedPreKey(5));

    keys.store(ACCOUNT_UUID, DEVICE_ID + 1,
        List.of(generateTestPreKey(6)),
        List.of(generateTestSignedPreKey(7)),
        generateTestSignedPreKey(8));

    assertEquals(2, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(2, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());

    keys.delete(ACCOUNT_UUID);

    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());
  }

  @Test
  void testDeleteByAccountAndDevice() {
    keys.store(ACCOUNT_UUID, DEVICE_ID,
        List.of(generateTestPreKey(1), generateTestPreKey(2)),
        List.of(generateTestSignedPreKey(3), generateTestSignedPreKey(4)),
        generateTestSignedPreKey(5));

    keys.store(ACCOUNT_UUID, DEVICE_ID + 1,
        List.of(generateTestPreKey(6)),
        List.of(generateTestSignedPreKey(7)),
        generateTestSignedPreKey(8));

    assertEquals(2, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(2, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());

    keys.delete(ACCOUNT_UUID, DEVICE_ID);

    assertEquals(0, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID));
    assertEquals(0, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID));
    assertFalse(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID).isPresent());
    assertEquals(1, keys.getEcCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertEquals(1, keys.getPqCount(ACCOUNT_UUID, DEVICE_ID + 1));
    assertTrue(keys.getLastResort(ACCOUNT_UUID, DEVICE_ID + 1).isPresent());
  }

  @Test
  void testStorePqLastResort() {
    assertEquals(0, getLastResortCount(ACCOUNT_UUID));

    final ECKeyPair identityKeyPair = Curve.generateKeyPair();

    keys.storePqLastResort(
        ACCOUNT_UUID,
        Map.of(1L, KeysHelper.signedKEMPreKey(1, identityKeyPair), 2L, KeysHelper.signedKEMPreKey(2, identityKeyPair)));
    assertEquals(2, getLastResortCount(ACCOUNT_UUID));
    assertEquals(1L, keys.getLastResort(ACCOUNT_UUID, 1L).get().getKeyId());
    assertEquals(2L, keys.getLastResort(ACCOUNT_UUID, 2L).get().getKeyId());
    assertFalse(keys.getLastResort(ACCOUNT_UUID, 3L).isPresent());

    keys.storePqLastResort(
        ACCOUNT_UUID,
        Map.of(1L, KeysHelper.signedKEMPreKey(3, identityKeyPair), 3L, KeysHelper.signedKEMPreKey(4, identityKeyPair)));
    assertEquals(3, getLastResortCount(ACCOUNT_UUID), "storing new last-resort keys should not create duplicates");
    assertEquals(3L, keys.getLastResort(ACCOUNT_UUID, 1L).get().getKeyId(), "storing new last-resort keys should overwrite old ones");
    assertEquals(2L, keys.getLastResort(ACCOUNT_UUID, 2L).get().getKeyId(), "storing new last-resort keys should leave untouched ones alone");
    assertEquals(4L, keys.getLastResort(ACCOUNT_UUID, 3L).get().getKeyId(), "storing new last-resort keys should overwrite old ones");
  }

  private int getLastResortCount(UUID uuid) {
    QueryRequest queryRequest = QueryRequest.builder()
        .tableName(Tables.PQ_LAST_RESORT_KEYS.tableName())
        .keyConditionExpression("#uuid = :uuid")
        .expressionAttributeNames(Map.of("#uuid", Keys.KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid)))
        .select(Select.COUNT)
        .build();
    QueryResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().query(queryRequest);
    return response.count();
  }

  @Test
  void testGetPqEnabledDevices() {
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();

    keys.store(ACCOUNT_UUID, DEVICE_ID, null, List.of(KeysHelper.signedKEMPreKey(1, identityKeyPair)), null);
    keys.store(ACCOUNT_UUID, DEVICE_ID + 1, null, null, KeysHelper.signedKEMPreKey(2, identityKeyPair));
    keys.store(ACCOUNT_UUID, DEVICE_ID + 2, null, List.of(KeysHelper.signedKEMPreKey(3, identityKeyPair)), KeysHelper.signedKEMPreKey(4, identityKeyPair));
    keys.store(ACCOUNT_UUID, DEVICE_ID + 3, null, null, null);
    assertIterableEquals(
        Set.of(DEVICE_ID + 1, DEVICE_ID + 2),
        Set.copyOf(keys.getPqEnabledDevices(ACCOUNT_UUID)));
  }

  @Test
  void testSortKeyPrefix() {
    AttributeValue got = Keys.getSortKeyPrefix(123);
    assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}, got.b().asByteArray());
  }

  @ParameterizedTest
  @MethodSource
  void extractByteArray(final AttributeValue attributeValue, final byte[] expectedByteArray) {
    assertArrayEquals(expectedByteArray, Keys.extractByteArray(attributeValue));
  }

  private static Stream<Arguments> extractByteArray() {
    final byte[] key = Base64.getDecoder().decode("c+k+8zv8WaFdDjR9IOvCk6BcY5OI7rge/YUDkaDGyRc=");

    return Stream.of(
        Arguments.of(AttributeValue.fromB(SdkBytes.fromByteArray(key)), key),
        Arguments.of(AttributeValue.fromS(Base64.getEncoder().encodeToString(key)), key),
        Arguments.of(AttributeValue.fromS(Base64.getEncoder().withoutPadding().encodeToString(key)), key)
    );
  }

  @ParameterizedTest
  @MethodSource
  void extractByteArrayIllegalArgument(final AttributeValue attributeValue) {
    assertThrows(IllegalArgumentException.class, () -> Keys.extractByteArray(attributeValue));
  }

  private static Stream<Arguments> extractByteArrayIllegalArgument() {
    return Stream.of(
        Arguments.of(AttributeValue.fromN("12")),
        Arguments.of(AttributeValue.fromS("")),
        Arguments.of(AttributeValue.fromS("Definitely not legitimate base64 👎"))
    );
  }

  private static PreKey generateTestPreKey(final long keyId) {
    final byte[] key = new byte[32];
    new SecureRandom().nextBytes(key);

    return new PreKey(keyId, key);
  }

  private static SignedPreKey generateTestSignedPreKey(final long keyId) {
    final byte[] key = new byte[32];
    final byte[] signature = new byte[32];

    final SecureRandom secureRandom = new SecureRandom();
    secureRandom.nextBytes(key);
    secureRandom.nextBytes(signature);

    return new SignedPreKey(keyId, key, signature);
  }
}
