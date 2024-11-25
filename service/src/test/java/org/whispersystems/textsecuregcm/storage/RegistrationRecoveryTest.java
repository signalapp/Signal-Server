/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

public class RegistrationRecoveryTest {

  private static final MutableClock CLOCK = MockUtils.mutableClock(0);
  private static final Duration EXPIRATION = Duration.ofSeconds(1000);
  private static final String NUMBER = "+18005555555";

  private static final SaltedTokenHash ORIGINAL_HASH = SaltedTokenHash.generateFor("pass1");
  private static final SaltedTokenHash ANOTHER_HASH = SaltedTokenHash.generateFor("pass2");

  @RegisterExtension
  private static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(Tables.PNI, Tables.REGISTRATION_RECOVERY_PASSWORDS);

  private UUID pni;

  private RegistrationRecoveryPasswords registrationRecoveryPasswords;

  private RegistrationRecoveryPasswordsManager manager;

  @BeforeEach
  public void before() throws Exception {
    CLOCK.setTimeMillis(Clock.systemUTC().millis());
    registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName(),
        EXPIRATION,
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        CLOCK
    );
    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.PNI.tableName());

    manager = new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords, phoneNumberIdentifiers);

    pni = phoneNumberIdentifiers.getPhoneNumberIdentifier(NUMBER).join();
  }

  @Test
  public void testLookupAfterWrite() throws Exception {
    registrationRecoveryPasswords.addOrReplace(NUMBER, pni, ORIGINAL_HASH).get();
    final long initialExp = fetchTimestamp(NUMBER);
    final long expectedExpiration = CLOCK.instant().getEpochSecond() + EXPIRATION.getSeconds();
    assertEquals(expectedExpiration, initialExp);

    final Optional<SaltedTokenHash> saltedTokenHashByPni = registrationRecoveryPasswords.lookup(pni).get();
    assertTrue(saltedTokenHashByPni.isPresent());
    assertEquals(ORIGINAL_HASH.salt(), saltedTokenHashByPni.get().salt());
    assertEquals(ORIGINAL_HASH.hash(), saltedTokenHashByPni.get().hash());
  }

  @Test
  public void testLookupAfterRefresh() throws Exception {
    registrationRecoveryPasswords.addOrReplace(NUMBER, pni, ORIGINAL_HASH).get();

    CLOCK.increment(50, TimeUnit.SECONDS);
    registrationRecoveryPasswords.addOrReplace(NUMBER, pni, ORIGINAL_HASH).get();
    final long updatedExp = fetchTimestamp(NUMBER);
    final long expectedExp = CLOCK.instant().getEpochSecond() + EXPIRATION.getSeconds();
    assertEquals(expectedExp, updatedExp);

    final Optional<SaltedTokenHash> saltedTokenHashByPni = registrationRecoveryPasswords.lookup(pni).get();
    assertTrue(saltedTokenHashByPni.isPresent());
    assertEquals(ORIGINAL_HASH.salt(), saltedTokenHashByPni.get().salt());
    assertEquals(ORIGINAL_HASH.hash(), saltedTokenHashByPni.get().hash());
  }

  @Test
  public void testReplace() throws Exception {
    registrationRecoveryPasswords.addOrReplace(NUMBER, pni, ORIGINAL_HASH).get();
    registrationRecoveryPasswords.addOrReplace(NUMBER, pni, ANOTHER_HASH).get();

    final Optional<SaltedTokenHash> saltedTokenHashByPni = registrationRecoveryPasswords.lookup(pni).get();
    assertTrue(saltedTokenHashByPni.isPresent());
    assertEquals(ANOTHER_HASH.salt(), saltedTokenHashByPni.get().salt());
    assertEquals(ANOTHER_HASH.hash(), saltedTokenHashByPni.get().hash());
  }

  @Test
  public void testRemove() throws Exception {
    assertDoesNotThrow(() -> registrationRecoveryPasswords.removeEntry(NUMBER, pni).join());

    registrationRecoveryPasswords.addOrReplace(NUMBER, pni, ORIGINAL_HASH).get();
    assertTrue(registrationRecoveryPasswords.lookup(pni).get().isPresent());

    registrationRecoveryPasswords.removeEntry(NUMBER, pni).get();
    assertTrue(registrationRecoveryPasswords.lookup(pni).get().isEmpty());
  }

  @Test
  public void testManagerFlow() throws Exception {
    final byte[] password = "password".getBytes(StandardCharsets.UTF_8);
    final byte[] updatedPassword = "udpate".getBytes(StandardCharsets.UTF_8);
    final byte[] wrongPassword = "qwerty123".getBytes(StandardCharsets.UTF_8);

    // initial store
    manager.storeForCurrentNumber(NUMBER, password).get();
    assertTrue(manager.verify(pni, password).get());
    assertFalse(manager.verify(pni, wrongPassword).get());

    // update
    manager.storeForCurrentNumber(NUMBER, password).get();
    assertTrue(manager.verify(pni, password).get());
    assertFalse(manager.verify(pni, wrongPassword).get());

    // replace
    manager.storeForCurrentNumber(NUMBER, updatedPassword).get();
    assertTrue(manager.verify(pni, updatedPassword).get());
    assertFalse(manager.verify(pni, password).get());
    assertFalse(manager.verify(pni, wrongPassword).get());

    manager.removeForNumber(NUMBER).get();
    assertFalse(manager.verify(pni, updatedPassword).get());
    assertFalse(manager.verify(pni, password).get());
    assertFalse(manager.verify(pni, wrongPassword).get());
  }

  private static long fetchTimestamp(final String number) throws ExecutionException, InterruptedException {
    return DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().getItem(GetItemRequest.builder()
            .tableName(Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName())
            .key(Map.of(RegistrationRecoveryPasswords.KEY_PNI, AttributeValues.fromString(number)))
            .build())
        .thenApply(getItemResponse -> {
          final Map<String, AttributeValue> item = getItemResponse.item();
          if (item == null || !item.containsKey(RegistrationRecoveryPasswords.ATTR_EXP)) {
            throw new RuntimeException("Data not found");
          }
          final String exp = item.get(RegistrationRecoveryPasswords.ATTR_EXP).n();
          return Long.parseLong(exp);
        })
        .get();
  }
}
