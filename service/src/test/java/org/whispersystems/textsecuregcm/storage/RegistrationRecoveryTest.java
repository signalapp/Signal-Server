/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class RegistrationRecoveryTest {

  private static final MutableClock CLOCK = MockUtils.mutableClock(0);

  private static final Duration EXPIRATION = Duration.ofSeconds(1000);

  private static final String NUMBER = "+18005555555";
  private static final UUID PNI = UUID.randomUUID();

  private static final SaltedTokenHash ORIGINAL_HASH = SaltedTokenHash.generateFor("pass1");

  private static final SaltedTokenHash ANOTHER_HASH = SaltedTokenHash.generateFor("pass2");

  @RegisterExtension
  private static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.PNI,
      Tables.REGISTRATION_RECOVERY_PASSWORDS);

  private RegistrationRecoveryPasswords registrationRecoveryPasswords;

  private RegistrationRecoveryPasswordsManager manager;

  @BeforeEach
  public void before() throws Exception {
    CLOCK.setTimeMillis(Clock.systemUTC().millis());
    registrationRecoveryPasswords = new RegistrationRecoveryPasswords(
        Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName(),
        EXPIRATION,
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        CLOCK
    );

    final PhoneNumberIdentifiers phoneNumberIdentifiers =
        new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), Tables.PNI.tableName());

    manager = new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords, phoneNumberIdentifiers);
  }

  @Test
  public void testLookupAfterWrite() throws Exception {
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).get();
    final long initialExp = fetchTimestamp(NUMBER);
    final long expectedExpiration = CLOCK.instant().getEpochSecond() + EXPIRATION.getSeconds();
    assertEquals(expectedExpiration, initialExp);

    {
      final Optional<SaltedTokenHash> saltedTokenHashByNumber = registrationRecoveryPasswords.lookup(NUMBER).get();
      assertTrue(saltedTokenHashByNumber.isPresent());
      assertEquals(ORIGINAL_HASH.salt(), saltedTokenHashByNumber.get().salt());
      assertEquals(ORIGINAL_HASH.hash(), saltedTokenHashByNumber.get().hash());
    }

    {
      final Optional<SaltedTokenHash> saltedTokenHashByPni = registrationRecoveryPasswords.lookup(PNI).get();
      assertTrue(saltedTokenHashByPni.isPresent());
      assertEquals(ORIGINAL_HASH.salt(), saltedTokenHashByPni.get().salt());
      assertEquals(ORIGINAL_HASH.hash(), saltedTokenHashByPni.get().hash());
    }
  }

  @Test
  public void testLookupAfterRefresh() throws Exception {
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).get();

    CLOCK.increment(50, TimeUnit.SECONDS);
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).get();
    final long updatedExp = fetchTimestamp(NUMBER);
    final long expectedExp = CLOCK.instant().getEpochSecond() + EXPIRATION.getSeconds();
    assertEquals(expectedExp, updatedExp);

    {
      final Optional<SaltedTokenHash> saltedTokenHashByNumber = registrationRecoveryPasswords.lookup(NUMBER).get();
      assertTrue(saltedTokenHashByNumber.isPresent());
      assertEquals(ORIGINAL_HASH.salt(), saltedTokenHashByNumber.get().salt());
      assertEquals(ORIGINAL_HASH.hash(), saltedTokenHashByNumber.get().hash());
    }

    {
      final Optional<SaltedTokenHash> saltedTokenHashByPni = registrationRecoveryPasswords.lookup(PNI).get();
      assertTrue(saltedTokenHashByPni.isPresent());
      assertEquals(ORIGINAL_HASH.salt(), saltedTokenHashByPni.get().salt());
      assertEquals(ORIGINAL_HASH.hash(), saltedTokenHashByPni.get().hash());
    }
  }

  @Test
  public void testReplace() throws Exception {
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).get();
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ANOTHER_HASH).get();

    {
      final Optional<SaltedTokenHash> saltedTokenHashByNumber = registrationRecoveryPasswords.lookup(NUMBER).get();
      assertTrue(saltedTokenHashByNumber.isPresent());
      assertEquals(ANOTHER_HASH.salt(), saltedTokenHashByNumber.get().salt());
      assertEquals(ANOTHER_HASH.hash(), saltedTokenHashByNumber.get().hash());
    }

    {
      final Optional<SaltedTokenHash> saltedTokenHashByPni = registrationRecoveryPasswords.lookup(PNI).get();
      assertTrue(saltedTokenHashByPni.isPresent());
      assertEquals(ANOTHER_HASH.salt(), saltedTokenHashByPni.get().salt());
      assertEquals(ANOTHER_HASH.hash(), saltedTokenHashByPni.get().hash());
    }
  }

  @Test
  public void testRemove() throws Exception {
    assertDoesNotThrow(() -> registrationRecoveryPasswords.removeEntry(NUMBER, PNI).join());

    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).get();
    assertTrue(registrationRecoveryPasswords.lookup(NUMBER).get().isPresent());

    registrationRecoveryPasswords.removeEntry(NUMBER, PNI).get();
    assertTrue(registrationRecoveryPasswords.lookup(NUMBER).get().isEmpty());
  }

  @Test
  public void testManagerFlow() throws Exception {
    final byte[] password = "password".getBytes(StandardCharsets.UTF_8);
    final byte[] updatedPassword = "udpate".getBytes(StandardCharsets.UTF_8);
    final byte[] wrongPassword = "qwerty123".getBytes(StandardCharsets.UTF_8);

    // initial store
    manager.storeForCurrentNumber(NUMBER, password).get();
    assertTrue(manager.verify(NUMBER, password).get());
    assertFalse(manager.verify(NUMBER, wrongPassword).get());

    // update
    manager.storeForCurrentNumber(NUMBER, password).get();
    assertTrue(manager.verify(NUMBER, password).get());
    assertFalse(manager.verify(NUMBER, wrongPassword).get());

    // replace
    manager.storeForCurrentNumber(NUMBER, updatedPassword).get();
    assertTrue(manager.verify(NUMBER, updatedPassword).get());
    assertFalse(manager.verify(NUMBER, password).get());
    assertFalse(manager.verify(NUMBER, wrongPassword).get());

    manager.removeForNumber(NUMBER).get();
    assertFalse(manager.verify(NUMBER, updatedPassword).get());
    assertFalse(manager.verify(NUMBER, password).get());
    assertFalse(manager.verify(NUMBER, wrongPassword).get());
  }

  @Test
  void getE164AssociatedRegistrationRecoveryPasswords() {
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).join();

    assertEquals(List.of(Tuples.of(NUMBER, ORIGINAL_HASH, registrationRecoveryPasswords.expirationSeconds())),
        registrationRecoveryPasswords.getE164AssociatedRegistrationRecoveryPasswords().collectList().block());
  }

  @Test
  void insertPniRecord() {
    final long expirationSeconds = Instant.now().plusSeconds(3600).getEpochSecond();

    DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
        .tableName(Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName())
        .item(Map.of(
            RegistrationRecoveryPasswords.KEY_E164, AttributeValues.fromString(NUMBER),
            RegistrationRecoveryPasswords.ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
            RegistrationRecoveryPasswords.ATTR_SALT, AttributeValues.fromString(ORIGINAL_HASH.salt()),
            RegistrationRecoveryPasswords.ATTR_HASH, AttributeValues.fromString(ORIGINAL_HASH.hash())))
        .build());

    assertTrue(registrationRecoveryPasswords.lookup(PNI).join().isEmpty());

    assertTrue(() -> registrationRecoveryPasswords.insertPniRecord(NUMBER, PNI, ORIGINAL_HASH, expirationSeconds).join());
    assertEquals(Optional.of(ORIGINAL_HASH), registrationRecoveryPasswords.lookup(PNI).join());
  }

  @Test
  void insertPniRecordOriginalDeleted() {
    final CompletionException completionException = assertThrows(CompletionException.class, () ->
        registrationRecoveryPasswords.insertPniRecord(NUMBER, PNI, ORIGINAL_HASH, 0L).join());

    assertInstanceOf(ContestedOptimisticLockException.class, completionException.getCause());
  }

  @Test
  void insertPniRecordOriginalChanged() {
    final long expirationSeconds = Instant.now().plusSeconds(3600).getEpochSecond();

    DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
        .tableName(Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName())
        .item(Map.of(
            RegistrationRecoveryPasswords.KEY_E164, AttributeValues.fromString(NUMBER),
            RegistrationRecoveryPasswords.ATTR_EXP, AttributeValues.fromLong(expirationSeconds),
            RegistrationRecoveryPasswords.ATTR_SALT, AttributeValues.fromString(ORIGINAL_HASH.salt()),
            RegistrationRecoveryPasswords.ATTR_HASH, AttributeValues.fromString(ORIGINAL_HASH.hash())))
        .build());

    final CompletionException completionException = assertThrows(CompletionException.class, () ->
        registrationRecoveryPasswords.insertPniRecord(NUMBER, PNI, ANOTHER_HASH, expirationSeconds).join());

    assertInstanceOf(ContestedOptimisticLockException.class, completionException.getCause());
  }

  @Test
  void insertPniRecordNewRecordAlreadyExists() {
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).join();

    assertTrue(registrationRecoveryPasswords.lookup(NUMBER).join().isPresent());
    assertTrue(registrationRecoveryPasswords.lookup(PNI).join().isPresent());
    assertEquals(registrationRecoveryPasswords.lookup(NUMBER).join(), registrationRecoveryPasswords.lookup(PNI).join());

    assertFalse(() ->
        registrationRecoveryPasswords.insertPniRecord(NUMBER, PNI, ORIGINAL_HASH, registrationRecoveryPasswords.expirationSeconds()).join());
  }

  @Test
  void insertPniRecordOriginalChangedNewRecordAlreadyExists() {
    registrationRecoveryPasswords.addOrReplace(NUMBER, PNI, ORIGINAL_HASH).join();

    assertTrue(registrationRecoveryPasswords.lookup(NUMBER).join().isPresent());
    assertTrue(registrationRecoveryPasswords.lookup(PNI).join().isPresent());
    assertEquals(registrationRecoveryPasswords.lookup(NUMBER).join(), registrationRecoveryPasswords.lookup(PNI).join());

    assertFalse(() ->
        registrationRecoveryPasswords.insertPniRecord(NUMBER, PNI, ANOTHER_HASH, registrationRecoveryPasswords.expirationSeconds()).join());
  }

  private static long fetchTimestamp(final String number) throws ExecutionException, InterruptedException {
    return DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().getItem(GetItemRequest.builder()
            .tableName(Tables.REGISTRATION_RECOVERY_PASSWORDS.tableName())
            .key(Map.of(RegistrationRecoveryPasswords.KEY_E164, AttributeValues.fromString(number)))
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

  @Test
  void migrateE164Record() {
    final RegistrationRecoveryPasswords registrationRecoveryPasswords = mock(RegistrationRecoveryPasswords.class);
    when(registrationRecoveryPasswords.insertPniRecord(any(), any(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(NUMBER)).thenReturn(CompletableFuture.completedFuture(PNI));

    final RegistrationRecoveryPasswordsManager migrationManager =
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords, phoneNumberIdentifiers);

    assertTrue(() -> migrationManager.migrateE164Record(NUMBER, ORIGINAL_HASH, 1234).join());
  }

  @Test
  void migrateE164RecordRetry() {
    final RegistrationRecoveryPasswords registrationRecoveryPasswords = mock(RegistrationRecoveryPasswords.class);

    when(registrationRecoveryPasswords.insertPniRecord(eq(NUMBER), eq(PNI), eq(ORIGINAL_HASH), anyLong()))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()));

    when(registrationRecoveryPasswords.insertPniRecord(eq(NUMBER), eq(PNI), eq(ANOTHER_HASH), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(true));

    when(registrationRecoveryPasswords.lookup(NUMBER))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ANOTHER_HASH)));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(NUMBER)).thenReturn(CompletableFuture.completedFuture(PNI));

    final RegistrationRecoveryPasswordsManager migrationManager =
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords, phoneNumberIdentifiers);

    assertTrue(() -> migrationManager.migrateE164Record(NUMBER, ORIGINAL_HASH, 1234).join());

    verify(registrationRecoveryPasswords).lookup(NUMBER);
    verify(registrationRecoveryPasswords, times(2)).insertPniRecord(any(), any(), any(), anyLong());
  }

  @Test
  void migrateE164RecordOriginalDeleted() {
    final RegistrationRecoveryPasswords registrationRecoveryPasswords = mock(RegistrationRecoveryPasswords.class);

    when(registrationRecoveryPasswords.insertPniRecord(eq(NUMBER), eq(PNI), eq(ORIGINAL_HASH), anyLong()))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()));

    when(registrationRecoveryPasswords.lookup(NUMBER))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(NUMBER)).thenReturn(CompletableFuture.completedFuture(PNI));

    final RegistrationRecoveryPasswordsManager migrationManager =
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords, phoneNumberIdentifiers);

    assertFalse(() -> migrationManager.migrateE164Record(NUMBER, ORIGINAL_HASH, 1234).join());

    verify(registrationRecoveryPasswords).lookup(NUMBER);
    verify(registrationRecoveryPasswords).insertPniRecord(any(), any(), any(), anyLong());
  }

  @Test
  void migrateE164RecordRetryExhausted() {
    final RegistrationRecoveryPasswords registrationRecoveryPasswords = mock(RegistrationRecoveryPasswords.class);

    when(registrationRecoveryPasswords.insertPniRecord(any(), any(), any(), anyLong()))
        .thenReturn(CompletableFuture.failedFuture(new ContestedOptimisticLockException()));

    when(registrationRecoveryPasswords.lookup(NUMBER))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(ORIGINAL_HASH)));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(NUMBER)).thenReturn(CompletableFuture.completedFuture(PNI));

    final RegistrationRecoveryPasswordsManager migrationManager =
        new RegistrationRecoveryPasswordsManager(registrationRecoveryPasswords, phoneNumberIdentifiers);

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> migrationManager.migrateE164Record(NUMBER, ORIGINAL_HASH, 1234).join());

    assertInstanceOf(ContestedOptimisticLockException.class, completionException.getCause());

    verify(registrationRecoveryPasswords, times(10)).lookup(NUMBER);
    verify(registrationRecoveryPasswords, times(10)).insertPniRecord(any(), any(), any(), anyLong());
  }
}
