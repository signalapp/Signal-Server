/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.storage.ReceiptCredentialTestUtil.receiptPresentation;
import static org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil.assertFailsWithCause;

import com.eatthepath.otp.HmacOneTimePasswordGenerator;
import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.ZkCredentialKeyPair;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AccountsTest {

  private static final byte DEVICE_ID_1 = 1;
  private static final byte DEVICE_ID_2 = 2;

  private static final String BASE_64_URL_USERNAME_HASH_1 = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_USERNAME_HASH_2 = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_1 = "md1votbj9r794DsqTNrBqA";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_2 = "9hrqVLy59bzgPse-S9NUsA";
  private static final byte[] USERNAME_HASH_1 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_1);
  private static final byte[] USERNAME_HASH_2 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_2);
  private static final byte[] ENCRYPTED_USERNAME_1 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_1);
  private static final byte[] ENCRYPTED_USERNAME_2 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_2);

  private static final AtomicInteger ACCOUNT_COUNTER = new AtomicInteger(1);

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      Tables.ACCOUNTS,
      Tables.NUMBERS,
      Tables.PNI_ASSIGNMENTS,
      Tables.USERNAMES,
      Tables.DELETED_ACCOUNTS,
      Tables.USED_LINK_DEVICE_TOKENS,
      Tables.REDEEMED_RECEIPTS,

      // This is an unrelated table used to test "tag-along" transactional updates
      Tables.CLIENT_RELEASES);

  private final TestClock clock = TestClock.pinned(Instant.EPOCH);
  private Accounts accounts;

  private record UsernameConstraint(UUID accountIdentifier, boolean confirmed, Optional<Instant> expiration) {
  }

  @BeforeEach
  void setupAccountsDao() {

    @SuppressWarnings("unchecked") DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(new DynamicConfiguration());

    clock.pin(Instant.EPOCH);
    accounts = new Accounts(
        clock,
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        new RedeemedReceiptsManager(clock, Tables.REDEEMED_RECEIPTS.tableName(),
            DYNAMO_DB_EXTENSION.getDynamoDbClient()),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  public void testStoreAndLookupUsernameLink(@Nullable final String number) throws Exception {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(), List.of(device));
    account.setUsernameHash(TestRandomUtil.nextBytes(16));

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));
    }

    final BiConsumer<Optional<Account>, byte[]> validator = (maybeAccount, expectedEncryptedUsername) -> {
      assertTrue(maybeAccount.isPresent());
      assertTrue(maybeAccount.get().getEncryptedUsername().isPresent());
      assertEquals(account.getAccountIdentifier(), maybeAccount.get().getAccountIdentifier());
      assertArrayEquals(expectedEncryptedUsername, maybeAccount.get().getEncryptedUsername().get());
    };

    // creating a username link, storing it, checking that it can be looked up
    final UUID linkHandle1 = UUID.randomUUID();
    final byte[] encruptedUsername1 = TestRandomUtil.nextBytes(32);
    account.setUsernameLinkDetails(linkHandle1, encruptedUsername1);
    accounts.update(account);
    validator.accept(accounts.getByUsernameLinkHandle(linkHandle1).join(), encruptedUsername1);

    // updating username link, storing new one, checking that it can be looked up, checking that old one can't be looked up
    final UUID linkHandle2 = UUID.randomUUID();
    final byte[] encruptedUsername2 = TestRandomUtil.nextBytes(32);
    account.setUsernameLinkDetails(linkHandle2, encruptedUsername2);
    accounts.update(account);
    validator.accept(accounts.getByUsernameLinkHandle(linkHandle2).join(), encruptedUsername2);
    assertTrue(accounts.getByUsernameLinkHandle(linkHandle1).join().isEmpty());

    // deleting username link, checking it can't be looked up by either handle
    account.setUsernameLinkDetails(null, null);
    accounts.update(account);
    assertTrue(accounts.getByUsernameLinkHandle(linkHandle1).join().isEmpty());
    assertTrue(accounts.getByUsernameLinkHandle(linkHandle2).join().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testStore(@Nullable final String number) throws Exception {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(), List.of(device));

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    boolean freshUser = number != null ? createAccount(account) : createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));

    assertThat(freshUser).isTrue();
    verifyStoredState(Optional.ofNullable(number), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, number != null);

    if (number != null) {
      assertThat(account.getPhoneNumberIdentifier()).isPresent();
      assertPhoneNumberConstraintExists(number, account.getAccountIdentifier());
      assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().get(), account.getAccountIdentifier());
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, account.getAccountIdentifier());
    }

    freshUser = number != null ? createAccount(account) : createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));

    assertThat(freshUser).isTrue();
    verifyStoredState(Optional.ofNullable(number), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, number != null);

    if (number != null) {
      assertThat(account.getPhoneNumberIdentifier()).isPresent();
      assertPhoneNumberConstraintExists(number, account.getAccountIdentifier());
      assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().get(), account.getAccountIdentifier());
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, account.getAccountIdentifier());
    }
  }

  @Test
  void testStoreRecentlyDeleted() {
    final UUID originalUuid = UUID.randomUUID();

    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", originalUuid, UUID.randomUUID(), List.of(device));

    boolean freshUser = createAccount(account);

    assertThat(freshUser).isTrue();
    verifyStoredState(Optional.of("+14151112222"), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().orElseThrow(), account.getAccountIdentifier());

    accounts.delete(originalUuid, Collections.emptyList());
    assertThat(accounts.findRecentlyDeletedAccountIdentifier(account.getPhoneNumberIdentifier().orElseThrow())).hasValue(originalUuid);

    freshUser = createAccount(account);
    assertThat(freshUser).isTrue();
    verifyStoredState(Optional.of("+14151112222"), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().orElseThrow(), account.getAccountIdentifier());

    assertThat(accounts.findRecentlyDeletedAccountIdentifier(account.getPhoneNumberIdentifier().orElseThrow())).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testStoreMulti(@Nullable final String number) throws Exception {
    final List<Device> devices = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));
    final Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(), devices);

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));
    }

    verifyStoredState(Optional.ofNullable(number), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, number != null);

    if (number != null) {
      assertThat(account.getPhoneNumberIdentifier()).isPresent();
      assertPhoneNumberConstraintExists(number, account.getAccountIdentifier());
      assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().get(), account.getAccountIdentifier());
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, account.getAccountIdentifier());
    }
  }

  @Test
  void testStoreAciCollisionFails() {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    boolean freshUser = createAccount(account);

    assertThat(freshUser).isTrue();
    verifyStoredState(Optional.of("+14151112222"), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().orElseThrow(), account.getAccountIdentifier());

    account.setNumber("+14153334444", UUID.randomUUID());
    assertThrows(IllegalArgumentException.class, () -> createAccount(account),
        "Reusing ACI with different PNI should fail");
  }

  @Test
  void testStorePniCollisionFails() {
    Device device1 = generateDevice(DEVICE_ID_1);
    Account account1 = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device1));

    boolean freshUser = createAccount(account1);

    assertThat(freshUser).isTrue();
    verifyStoredState(Optional.of("+14151112222"), account1.getAccountIdentifier(), account1.getPhoneNumberIdentifier(), null, account1, true);

    assertPhoneNumberConstraintExists("+14151112222", account1.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(account1.getPhoneNumberIdentifier().orElseThrow(), account1.getAccountIdentifier());

    Device device2 = generateDevice(DEVICE_ID_1);
    Account account2 = generateAccount("+14151112222", UUID.randomUUID(), account1.getPhoneNumberIdentifier().orElseThrow(),
        List.of(device2));

    assertThrows(AccountAlreadyExistsException.class, () -> accounts.create(account2, Collections.emptyList()),
        "New ACI with same PNI should fail");
  }

  @Test
  void testRetrieve() {
    final List<Device> devicesFirst = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));

    UUID uuidFirst = UUID.randomUUID();
    UUID pniFirst = UUID.randomUUID();
    Account accountFirst = generateAccount("+14151112222", uuidFirst, pniFirst, devicesFirst);

    final List<Device> devicesSecond = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));

    UUID uuidSecond = UUID.randomUUID();
    UUID pniSecond = UUID.randomUUID();
    Account accountSecond = generateAccount("+14152221111", uuidSecond, pniSecond, devicesSecond);

    createAccount(accountFirst);
    createAccount(accountSecond);

    Optional<Account> retrievedFirst = accounts.getByE164("+14151112222");
    Optional<Account> retrievedSecond = accounts.getByE164("+14152221111");

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState(Optional.of("+14151112222"), uuidFirst, Optional.of(pniFirst), null, retrievedFirst.get(), accountFirst);
    verifyStoredState(Optional.of("+14152221111"), uuidSecond, Optional.of(pniSecond), null, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByAccountIdentifier(uuidFirst);
    retrievedSecond = accounts.getByAccountIdentifier(uuidSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState(Optional.of("+14151112222"), uuidFirst, Optional.of(pniFirst), null, retrievedFirst.get(), accountFirst);
    verifyStoredState(Optional.of("+14152221111"), uuidSecond, Optional.of(pniSecond), null, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByPhoneNumberIdentifier(pniFirst);
    retrievedSecond = accounts.getByPhoneNumberIdentifier(pniSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState(Optional.of("+14151112222"), uuidFirst, Optional.of(pniFirst), null, retrievedFirst.get(), accountFirst);
    verifyStoredState(Optional.of("+14152221111"), uuidSecond, Optional.of(pniSecond), null, retrievedSecond.get(), accountSecond);
  }

  @Test
  void testRetrieveNumberlessAccount() throws Exception {
    final List<Device> devicesFirst = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));

    final UUID uuidFirst = UUID.randomUUID();
    final Account accountFirst = generateAccount(null, uuidFirst, null, devicesFirst);

    final List<Device> devicesSecond = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));

    final UUID uuidSecond = UUID.randomUUID();
    final Account accountSecond = generateAccount(null, uuidSecond, null, devicesSecond);

    createNumberlessAccount(accountFirst, receiptPresentation(), TestRandomUtil.nextBytes(16));
    createNumberlessAccount(accountSecond, receiptPresentation(), TestRandomUtil.nextBytes(16));

    final Optional<Account> retrievedFirst = accounts.getByAccountIdentifier(uuidFirst);
    final Optional<Account> retrievedSecond = accounts.getByAccountIdentifier(uuidSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState(Optional.empty(), uuidFirst, Optional.empty(), null, retrievedFirst.get(), accountFirst);
    verifyStoredState(Optional.empty(), uuidSecond, Optional.empty(), null, retrievedSecond.get(), accountSecond);
  }

  // State before the account is re-registered
  enum UsernameStatus {
    NONE,
    RESERVED,
    RESERVED_WITH_SAVED_LINK,
    CONFIRMED
  }

  @CartesianTest
  @CartesianTest.MethodFactory("reclaimAccountWithNoUsername")
  void reclaimAccountWithNoUsername(final UsernameStatus usernameStatus, @Nullable final String number)
      throws Exception {
    Device device = generateDevice(DEVICE_ID_1);
    UUID firstUuid = UUID.randomUUID();
    final byte[] accoutRecoveryPassword = TestRandomUtil.nextBytes(16);
    Account account = generateAccount(number, firstUuid, number == null ? null : UUID.randomUUID(), List.of(device), accoutRecoveryPassword);

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptCredentialPresentation, accoutRecoveryPassword);
    }

    final byte[] usernameHash = TestRandomUtil.nextBytes(32);
    final byte[] encryptedUsername = TestRandomUtil.nextBytes(32);
    switch (usernameStatus) {
      case NONE:
        break;
      case RESERVED:
        accounts.reserveUsernameHash(account, TestRandomUtil.nextBytes(32), Duration.ofMinutes(1));
        break;
      case RESERVED_WITH_SAVED_LINK:
        // give the account a username
        accounts.reserveUsernameHash(account, usernameHash, Duration.ofMinutes(1));
        accounts.confirmUsernameHash(account, usernameHash, encryptedUsername);

        // simulate a partially-completed re-reg: we give the account a reclaimable username, but we'll try
        // re-registering again later in the test case
        account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
            List.of(generateDevice(DEVICE_ID_1)), accoutRecoveryPassword);
        if (number != null) {
          reclaimAccount(account);
        } else {
          reclaimNumberlessAccount(account, receiptCredentialPresentation, accoutRecoveryPassword);
        }
        break;
      case CONFIRMED:
        accounts.reserveUsernameHash(account, usernameHash, Duration.ofMinutes(1));
        accounts.confirmUsernameHash(account, usernameHash, encryptedUsername);
        break;
    }

    Optional<UUID> preservedLink = Optional.ofNullable(account.getUsernameLinkHandle());

    // re-register the account
    account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
        List.of(generateDevice(DEVICE_ID_1)));
    if (number != null) {
      reclaimAccount(account);
    } else {
      reclaimNumberlessAccount(account, receiptCredentialPresentation, accoutRecoveryPassword);
    }

    // If we had a username link, or we had previously saved a username link from another re-registration, make sure
    // we preserve it
    accounts.confirmUsernameHash(account, usernameHash, encryptedUsername);

    boolean shouldReuseLink = switch (usernameStatus) {
      case RESERVED_WITH_SAVED_LINK, CONFIRMED -> true;
      case NONE, RESERVED -> false;
    };

    // If we had a reclaimable username, make sure we preserved the link.
    assertThat(Objects.equals(account.getUsernameLinkHandle(), preservedLink.orElse(null)))
        .isEqualTo(shouldReuseLink);

    // in all cases, we should now have usernameHash, usernameLink, and encryptedUsername set
    assertThat(account.getUsernameHash()).isNotEmpty();
    assertThat(account.getEncryptedUsername()).isNotEmpty();
    assertThat(account.getUsernameLinkHandle()).isNotNull();
    assertThat(account.getReservedUsernameHash()).isEmpty();
  }

  static ArgumentSets reclaimAccountWithNoUsername() {
    return ArgumentSets.argumentsForFirstParameter(UsernameStatus.values())
        // number
        .argumentsForNextParameter("+14151112222", null);
  }

  private void reclaimAccount(final Account reregisteredAccount) {
    final AccountAlreadyExistsException accountAlreadyExistsException =
        assertThrows(AccountAlreadyExistsException.class,
            () -> accounts.create(reregisteredAccount, Collections.emptyList()));

    reregisteredAccount.setAccountIdentifier(accountAlreadyExistsException.getExistingAccount().getAccountIdentifier());

    // Phone number canonicalization means that a user can re-register with a different phone number
    // in the same equivalence class and get back the same phone number identifier.
    // In that case, we favor the re-registering account's phone number.
    reregisteredAccount.setNumber(reregisteredAccount.getNumber().orElseThrow(),
        accountAlreadyExistsException.getExistingAccount().getPhoneNumberIdentifier().orElseThrow());

    assertDoesNotThrow(() -> accounts.reclaimAccount(accountAlreadyExistsException.getExistingAccount(),
        reregisteredAccount,
        Collections.emptyList()).toCompletableFuture().join());
  }

  private void reclaimNumberlessAccount(final Account reregisteredAccount, final ReceiptCredentialPresentation receiptCredentialPresentation, final byte[] accountRecoveryPassword) {
    final AccountAlreadyExistsException accountAlreadyExistsException =
        assertThrows(AccountAlreadyExistsException.class,
            () -> accounts.create(reregisteredAccount, receiptCredentialPresentation, accountRecoveryPassword, Collections.emptyList()));

    reregisteredAccount.setAccountIdentifier(accountAlreadyExistsException.getExistingAccount().getAccountIdentifier());

    assertDoesNotThrow(() -> accounts.reclaimAccount(accountAlreadyExistsException.getExistingAccount(),
        reregisteredAccount,
        Collections.emptyList()).toCompletableFuture().join());
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testReclaimAccountPreservesBackupCredentialFields(@Nullable final String number) throws Exception {
    final UUID existingUuid = UUID.randomUUID();
    final byte[] accountRecoveryPassword = TestRandomUtil.nextBytes(16);
    final Account existingAccount =
        generateAccount(number, existingUuid, number == null ? null : UUID.randomUUID(),
            List.of(generateDevice(DEVICE_ID_1)), accountRecoveryPassword);

    // the backup credential request and share-set are always preserved across account reclaims
    existingAccount.setBackupCredentialRequests(TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(32));
    existingAccount.setZkCredentialKey(ZkCredentialKeyPair.generate().getPublicKey());

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(existingAccount);
    } else {
      createNumberlessAccount(existingAccount, receiptCredentialPresentation, accountRecoveryPassword);
    }

    final Account secondAccount =
        generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
            List.of(generateDevice(DEVICE_ID_1)), accountRecoveryPassword);

    if (number != null) {
      reclaimAccount(secondAccount);
    } else {
      reclaimNumberlessAccount(secondAccount, receiptCredentialPresentation, accountRecoveryPassword);
    }

    final Account reclaimed = accounts.getByAccountIdentifier(existingUuid).orElseThrow();
    assertThat(reclaimed.getBackupCredentialRequest(BackupCredentialType.MESSAGES).orElseThrow())
        .isEqualTo(existingAccount.getBackupCredentialRequest(BackupCredentialType.MESSAGES).orElseThrow());
    assertThat(reclaimed.getBackupCredentialRequest(BackupCredentialType.MEDIA).orElseThrow())
        .isEqualTo(existingAccount.getBackupCredentialRequest(BackupCredentialType.MEDIA).orElseThrow());
    assertThat(reclaimed.getZkCredentialKey()).hasValue(existingAccount.getZkCredentialKey().orElseThrow());
  }

  @Test
  void testIdempotentCreationWithMfaFails() throws Exception {
    final UUID existingUuid = UUID.randomUUID();
    final byte[] accountRecoveryPassword = TestRandomUtil.nextBytes(16);
    final Account existingAccount =
        generateAccount(null, existingUuid, null, List.of(generateDevice(DEVICE_ID_1)), accountRecoveryPassword);

    existingAccount.setMfaKeys(Map.of((byte) 1, new AnnotatedTotpKey(new TotpKey(
        new TotpParameters(
            TimeBasedOneTimePasswordGenerator.TOTP_ALGORITHM_HMAC_SHA1,
            HmacOneTimePasswordGenerator.DEFAULT_PASSWORD_LENGTH,
            TimeBasedOneTimePasswordGenerator.DEFAULT_TIME_STEP),
        TestRandomUtil.nextBytes(16)),
        TestRandomUtil.nextBytes(16))));

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    createNumberlessAccount(existingAccount, receiptCredentialPresentation, accountRecoveryPassword);

    final Account secondAccount =
        generateAccount(null, UUID.randomUUID(), null, List.of(generateDevice(DEVICE_ID_1)), accountRecoveryPassword);
    assertThrows(ReceiptAlreadyRedeemedException.class,
        () -> accounts.create(secondAccount, receiptCredentialPresentation, accountRecoveryPassword,
            Collections.emptyList()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testReclaimAccountPreservesTotpKeys(@Nullable final String number) throws Exception {
    final UUID existingUuid = UUID.randomUUID();
    final byte[] accountRecoveryPassword = TestRandomUtil.nextBytes(16);
    final Account existingAccount =
        generateAccount(number, existingUuid, number == null ? null : UUID.randomUUID(),
            List.of(generateDevice(DEVICE_ID_1)), accountRecoveryPassword);

    existingAccount.setMfaKeys(Map.of((byte) 1, new AnnotatedTotpKey(new TotpKey(
        new TotpParameters(
            TimeBasedOneTimePasswordGenerator.TOTP_ALGORITHM_HMAC_SHA1,
            HmacOneTimePasswordGenerator.DEFAULT_PASSWORD_LENGTH,
            TimeBasedOneTimePasswordGenerator.DEFAULT_TIME_STEP),
        TestRandomUtil.nextBytes(16)),
        TestRandomUtil.nextBytes(16))));

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(existingAccount);
    } else {
      createNumberlessAccount(existingAccount, receiptCredentialPresentation, accountRecoveryPassword);
    }

    final Account secondAccount =
        generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
            List.of(generateDevice(DEVICE_ID_1)), accountRecoveryPassword);

    if (number != null) {
      reclaimAccount(secondAccount);
    } else {
      // There are essentially two modes of 'reclamation' for a numberless account:
      // - Try to create an account and then reclaim it on conflict to allow for idempotent retries of creation
      // - Explicitly recover the account, since we know from the API if the client is trying to recover
      // Only the second case works here, because we forbid idempotent retries if the account has already set an
      // MFA key.
      secondAccount.setAccountIdentifier(existingAccount.getAccountIdentifier());
      accounts.reclaimAccount(existingAccount,
          secondAccount,
          Collections.emptyList()).toCompletableFuture().join();
    }

    final Account reclaimed = accounts.getByAccountIdentifier(existingUuid).orElseThrow();

    assertThat(reclaimed.getMfaKeys()).isEqualTo(existingAccount.getMfaKeys());
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testReclaimAccount(@Nullable final String number) throws Exception {
    final Device device = generateDevice(DEVICE_ID_1);
    final UUID existingUuid = UUID.randomUUID();
    final UUID existingPni = number == null ? null : UUID.randomUUID();
    final byte[] accountRecoveryPassword = TestRandomUtil.nextBytes(16);
    final Account existingAccount = generateAccount(number, existingUuid, existingPni, List.of(device), accountRecoveryPassword);

    // Backup vouchers should be carried over across re-registration
    final Account.BackupVoucher bv = new Account.BackupVoucher(1, Instant.now().plus(Duration.ofDays(1)));
    existingAccount.setBackupVoucher(bv);
    // ZK credential keys should be carried over across re-registration
    existingAccount.setZkCredentialKey(ZkCredentialKeyPair.generate().getPublicKey());

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(existingAccount);
    } else {
      createNumberlessAccount(existingAccount, receiptCredentialPresentation, accountRecoveryPassword);
    }

    final byte[] usernameHash = TestRandomUtil.nextBytes(32);
    final byte[] encryptedUsername = TestRandomUtil.nextBytes(16);

    // Set up the existing account to have a username hash
    accounts.confirmUsernameHash(existingAccount, usernameHash, encryptedUsername);
    final UUID usernameLinkHandle = existingAccount.getUsernameLinkHandle();

    verifyStoredState(Optional.ofNullable(number), existingAccount.getAccountIdentifier(),
        existingAccount.getPhoneNumberIdentifier(), usernameHash, existingAccount,
        number != null);

    if (number != null) {
      assertThat(existingAccount.getPhoneNumberIdentifier()).isPresent();
      assertPhoneNumberConstraintExists(number, existingUuid);
      assertPhoneNumberIdentifierConstraintExists(existingPni, existingUuid);
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, existingUuid);
    }

    assertDoesNotThrow(() -> accounts.update(existingAccount));

    final UUID secondUuid = UUID.randomUUID();

    final Device secondDevice = generateDevice(DEVICE_ID_1);
    final Account secondAccount =
        generateAccount(number, secondUuid, number == null ? null : UUID.randomUUID(), List.of(secondDevice));

    if (number != null) {
      reclaimAccount(secondAccount);
    } else {
      reclaimNumberlessAccount(secondAccount, receiptCredentialPresentation, accountRecoveryPassword);
    }

    // usernameHash should be unset
    verifyStoredState(Optional.ofNullable(number), existingUuid, Optional.ofNullable(existingPni), null, secondAccount, number != null);

    // username should become 'reclaimable'
    Map<String, AttributeValue> item = readAccount(existingUuid);
    Account result = Accounts.fromItem(item);
    assertThat(AttributeValues.getUUID(item, Accounts.ATTR_USERNAME_LINK_UUID, null))
        .isEqualTo(usernameLinkHandle)
        .isEqualTo(result.getUsernameLinkHandle());
    assertThat(result.getUsernameHash()).isEmpty();
    assertThat(result.getEncryptedUsername()).isEmpty();
    assertArrayEquals(result.getReservedUsernameHash().orElseThrow(), usernameHash);

    assertThat(result.getBackupVoucher()).isEqualTo(bv);

    // should keep the same usernameLink, now encryptedUsername should be set
    accounts.confirmUsernameHash(result, usernameHash, encryptedUsername);
    item = readAccount(existingUuid);
    result = Accounts.fromItem(item);
    assertThat(AttributeValues.getUUID(item, Accounts.ATTR_USERNAME_LINK_UUID, null))
        .isEqualTo(usernameLinkHandle)
        .isEqualTo(result.getUsernameLinkHandle());
    assertArrayEquals(encryptedUsername, result.getEncryptedUsername().orElseThrow());
    assertArrayEquals(usernameHash, result.getUsernameHash().orElseThrow());
    assertThat(result.getReservedUsernameHash()).isEmpty();

    if (number != null) {
      assertPhoneNumberConstraintExists(number, existingUuid);
      assertPhoneNumberIdentifierConstraintExists(existingPni, existingUuid);

      final Account invalidAccount =
          generateAccount("+14151113333", existingUuid, UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

      assertThatThrownBy(() -> createAccount(invalidAccount));
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, existingUuid);
    }
  }

  @ParameterizedTest
  @MethodSource
  void testReclaimAccountEquivalentPhoneNumbers(final String firstNumber, final String secondNumber) throws IOException {
    final UUID existingUuid = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();
    final Account existingAccount = generateAccount(firstNumber, existingUuid, pni, List.of(generateDevice(DEVICE_ID_1)));

    createAccount(existingAccount);

    assertTrue(existingAccount.getPhoneNumberIdentifier().isPresent());
    verifyStoredState(Optional.of(firstNumber), existingAccount.getAccountIdentifier(), existingAccount.getPhoneNumberIdentifier(), null, existingAccount, true);

    assertPhoneNumberConstraintExists(firstNumber, existingUuid);
    assertPhoneNumberIdentifierConstraintExists(pni, existingUuid);

    assertDoesNotThrow(() -> accounts.update(existingAccount));

    final UUID secondUuid = UUID.randomUUID();

    final Account secondAccount = generateAccount(secondNumber, secondUuid, pni, List.of(generateDevice(DEVICE_ID_1)));

    reclaimAccount(secondAccount);

    Map<String, AttributeValue> item = readAccount(existingUuid);
    final Account account = SystemMapper.jsonMapper().readValue(item.get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), Account.class);

    assertTrue(account.getNumber().isPresent());
    assertThat(AttributeValues.getString(item, Accounts.ATTR_ACCOUNT_E164, null))
        .isEqualTo(secondNumber)
        .isEqualTo(account.getNumber().get());
    assertPhoneNumberConstraintDoesNotExist(firstNumber);
    assertPhoneNumberConstraintExists(secondNumber, existingUuid);
    assertPhoneNumberIdentifierConstraintExists(pni, existingUuid);
  }

  private static Stream<Arguments> testReclaimAccountEquivalentPhoneNumbers() {
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");
    return Stream.of(
        Arguments.of(newFormatBeninE164, oldFormatBeninE164),
        Arguments.of(oldFormatBeninE164, newFormatBeninE164)
    );
  }

  @Test
  void testReclaimAccountNonEquivalentPhoneNumbers() {
    final String beninPhoneNumber = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final UUID existingUuid = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();
    final Account existingAccount = generateAccount(beninPhoneNumber, existingUuid, pni, List.of(generateDevice(DEVICE_ID_1)));

    createAccount(existingAccount);

    assertTrue(existingAccount.getPhoneNumberIdentifier().isPresent());
    verifyStoredState(Optional.of(beninPhoneNumber), existingAccount.getAccountIdentifier(), existingAccount.getPhoneNumberIdentifier(), null, existingAccount, true);

    assertPhoneNumberConstraintExists(beninPhoneNumber, existingUuid);
    assertPhoneNumberIdentifierConstraintExists(pni, existingUuid);

    assertDoesNotThrow(() -> accounts.update(existingAccount));

    final String usPhoneNumber = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final UUID secondUuid = UUID.randomUUID();

    // A non-equivalent phone number with the same PNI should fail reclamation
    final Account secondAccount = generateAccount(usPhoneNumber, secondUuid, pni, List.of(generateDevice(DEVICE_ID_1)));

    final AccountAlreadyExistsException accountAlreadyExistsException =
        assertThrows(AccountAlreadyExistsException.class,
            () -> accounts.create(secondAccount, Collections.emptyList()));

    secondAccount.setAccountIdentifier(accountAlreadyExistsException.getExistingAccount().getAccountIdentifier());

    assertThrows(IllegalStateException.class, () -> accounts.reclaimAccount(existingAccount,
        secondAccount,
        Collections.emptyList()).toCompletableFuture().join());
  }

  @Test
  void testReclaimAccountUnexpectedDatabasePhoneNumber() {
    final String beninPhoneNumber = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final UUID existingUuid = UUID.randomUUID();
    final UUID existingPni = UUID.randomUUID();
    final Account existingAccount = generateAccount(beninPhoneNumber, existingUuid, existingPni, List.of(generateDevice(DEVICE_ID_1)));

    createAccount(existingAccount);

    assertTrue(existingAccount.getPhoneNumberIdentifier().isPresent());
    verifyStoredState(Optional.of(beninPhoneNumber), existingAccount.getAccountIdentifier(), existingAccount.getPhoneNumberIdentifier(), null, existingAccount, true);

    assertPhoneNumberConstraintExists(beninPhoneNumber, existingUuid);
    assertPhoneNumberIdentifierConstraintExists(existingPni, existingUuid);

    assertDoesNotThrow(() -> accounts.update(existingAccount));

    final String usPhoneNumber = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final Account secondAccount = generateAccount(usPhoneNumber, existingUuid, existingPni, List.of(generateDevice(DEVICE_ID_1)));

    // This scenario is very contrived but tests our error handling if we somehow use an existing account with a different
    // phone number than what actually exists in the database.
    assertFailsWithCause(UnexpectedExistingPhoneNumberException.class, accounts.reclaimAccount(secondAccount, secondAccount,
        Collections.emptyList()).toCompletableFuture());

  }

  @Test
  void testReclaimAccountExistingAccountVersionChange() {
    final Device device = generateDevice(DEVICE_ID_1);
    final String number = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final UUID existingUuid = UUID.randomUUID();
    final UUID existingPni = UUID.randomUUID();
    final byte[] accountRecoveryPassword = TestRandomUtil.nextBytes(16);
    final Account existingAccount = generateAccount(number, existingUuid, existingPni, List.of(device), accountRecoveryPassword);

    createAccount(existingAccount);

    // Update the existing account without directly modifying the in-memory version so the stored version is out of
    // sync with the in-memory version
    {
      final Account updatedExistingAccount = accounts.getByAccountIdentifier(existingUuid).orElseThrow();
      updatedExistingAccount.setUnrestrictedUnidentifiedAccess(!existingAccount.isUnrestrictedUnidentifiedAccess());

      accounts.update(updatedExistingAccount);
    }

    final Account reclaimedAccount = new Account();
    reclaimedAccount.setAccountIdentifier(existingAccount.getAccountIdentifier());
    reclaimedAccount.setNumber(number, existingPni);

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> accounts.reclaimAccount(existingAccount, reclaimedAccount, Collections.emptyList()).toCompletableFuture().join());

    assertInstanceOf(ContestedOptimisticLockException.class, completionException.getCause());
  }

  @Test
  void testUpdateAccountWithMismatchedJsonDdbPhoneNumbers() {
    // Test that fixing the DynamoDB/JSON phone number mismatch does not break account updates for existing accounts
    // with bad data in the time after we ship this change and before we run the crawler to fix the mismatch.
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");
    final UUID existingUuid = UUID.randomUUID();
    final UUID existingPni = UUID.randomUUID();
    final Account existingAccount = generateAccount(newFormatBeninE164, existingUuid, existingPni, List.of(generateDevice(DEVICE_ID_1)));

    createAccount(existingAccount);

    verifyStoredState(Optional.of(newFormatBeninE164), existingAccount.getAccountIdentifier(), existingAccount.getPhoneNumberIdentifier(), null, existingAccount, true);

    assertPhoneNumberConstraintExists(newFormatBeninE164, existingUuid);
    assertPhoneNumberIdentifierConstraintExists(existingPni, existingUuid);

    // Mimic the current bad state
    DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().updateItem(UpdateItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(existingUuid)))
        .updateExpression("SET #number = :old_number")
        .expressionAttributeNames(Map.of("#number", Accounts.ATTR_ACCOUNT_E164))
        .expressionAttributeValues(
            Map.of(":old_number", AttributeValues.fromString(oldFormatBeninE164)))
        .build())
        .join();

    assertDoesNotThrow(() -> accounts.update(existingAccount));
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testUpdate(@Nullable final String number) throws Exception {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
        List.of(device));

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));
    }

    if (number != null) {
      assertThat(account.getPhoneNumberIdentifier()).isPresent();
      assertPhoneNumberConstraintExists(number, account.getAccountIdentifier());
      assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().get(), account.getAccountIdentifier());
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, account.getAccountIdentifier());
    }

    device.setName("foobar".getBytes(StandardCharsets.UTF_8));

    accounts.update(account);

    if (number != null) {
      assertThat(account.getPhoneNumberIdentifier()).isPresent();
      assertPhoneNumberConstraintExists(number, account.getAccountIdentifier());
      assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier().get(), account.getAccountIdentifier());
    } else {
      assertRedeemedReceiptConstraintExists(receiptCredentialPresentation, account.getAccountIdentifier());
    }

    if (number != null) {
      assertThat(account.getPhoneNumberIdentifier()).isPresent();
      final Optional<Account> retrievedByE164 = accounts.getByE164(number);

      assertThat(retrievedByE164).isPresent();
      verifyStoredState(Optional.of(number), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null,
          retrievedByE164.get(), account);
    }

    final Optional<Account> retrieved = accounts.getByAccountIdentifier(account.getAccountIdentifier());

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState(Optional.ofNullable(number), account.getAccountIdentifier(),
        account.getPhoneNumberIdentifier(), null, account, number != null);

    device = generateDevice(DEVICE_ID_1);
    Account unknownAccount = generateAccount(number == null ? null : "+14151113333", UUID.randomUUID(),
        number == null ? null : UUID.randomUUID(), List.of(device));

    assertThatThrownBy(() -> accounts.update(unknownAccount)).isInstanceOfAny(ConditionalCheckFailedException.class);

    accounts.update(account);

    assertThat(account.getVersion()).isEqualTo(2);

    verifyStoredState(Optional.ofNullable(number), account.getAccountIdentifier(),
        account.getPhoneNumberIdentifier(), null, account, number != null);

    account.setVersion(1);

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);

    account.setVersion(2);

    accounts.update(account);

    verifyStoredState(Optional.ofNullable(number), account.getAccountIdentifier(),
        account.getPhoneNumberIdentifier(), null, account, number != null);
  }

  @ParameterizedTest
  @ValueSource(strings = "+14151112222")
  @NullSource
  void testUpdateWithMockTransactionConflictException(@Nullable final String number) {

    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
    accounts = new Accounts(
        clock,
        dynamoDbClient,
        mock(DynamoDbAsyncClient.class),
        new RedeemedReceiptsManager(clock, Tables.REDEEMED_RECEIPTS.tableName(),
            dynamoDbClient),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());

    when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(TransactionConflictException.builder().build());

    final Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID());

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testUpdateTransactionally(@Nullable final String number) throws Exception {
    final Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID());

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));
    }

    final byte[] deviceName = "device-name".getBytes(StandardCharsets.UTF_8);

    assertNotEquals(deviceName,
        accounts.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getPrimaryDevice().getName());

    assertFalse(DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(Tables.CLIENT_RELEASES.tableName())
            .key(Map.of(
                ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
            ))
            .build())
        .hasItem());

    account.getPrimaryDevice().setName(deviceName);

    accounts.updateTransactionally(account, List.of(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(Tables.CLIENT_RELEASES.tableName())
            .item(Map.of(
                ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
            ))
            .build())
        .build()));

    assertArrayEquals(deviceName,
        accounts.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow().getPrimaryDevice().getName());

    assertTrue(DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(Tables.CLIENT_RELEASES.tableName())
            .key(Map.of(
                ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
            ))
            .build())
        .hasItem());
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testUpdateTransactionallyContestedLock(@Nullable final String number) throws Exception {
    final Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID());

    final ReceiptCredentialPresentation receiptCredentialPresentation = receiptPresentation();
    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptCredentialPresentation, TestRandomUtil.nextBytes(16));
    }

    account.setVersion(account.getVersion() - 1);

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.updateTransactionally(account, List.of(TransactWriteItem.builder()
            .put(Put.builder()
                .tableName(Tables.CLIENT_RELEASES.tableName())
                .item(Map.of(
                    ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                    ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
                ))
                .build())
            .build())));
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testUpdateTransactionallyWithMockTransactionConflictException(@Nullable final String number) {
    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    accounts = new Accounts(
        clock,
        dynamoDbClient,
        mock(DynamoDbAsyncClient.class),
        new RedeemedReceiptsManager(clock, Tables.REDEEMED_RECEIPTS.tableName(),
            dynamoDbClient),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());

    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .cancellationReasons(CancellationReason.builder()
                .code("TransactionConflict")
                .build())
            .build());

    final Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID());

    assertThatThrownBy(() -> accounts.updateTransactionally(account, Collections.emptyList()))
        .isInstanceOfAny(ContestedOptimisticLockException.class);
  }

  @Test
  void testGetAll() throws Exception {
    final List<Account> expectedAccounts = new ArrayList<>();

    for (int i = 1; i <= 100; i++) {
      final boolean numberless = i % 2 == 0;

      final Account account = numberless
          ? generateNumberlessAccount(UUID.randomUUID())
          : generateAccount("+1" + String.format("%03d", i), UUID.randomUUID(), UUID.randomUUID());

      expectedAccounts.add(account);

      if (numberless) {
        createNumberlessAccount(account, receiptPresentation(), TestRandomUtil.nextBytes(16));
      } else {
        createAccount(account);
      }
    }

    final List<Account> retrievedAccounts =
        accounts.getAll(2, Schedulers.parallel()).collectList().block();

    assertNotNull(retrievedAccounts);
    assertEquals(expectedAccounts.stream().map(Account::getAccountIdentifier).collect(Collectors.toSet()),
        retrievedAccounts.stream().map(Account::getAccountIdentifier).collect(Collectors.toSet()));
  }

  @Test
  void testGetAllAccountIdentifiers() throws Exception {
    final Set<UUID> expectedAccountIdentifiers = new HashSet<>();

    for (int i = 1; i <= 100; i++) {
      final boolean numberless = i % 2 == 0;

      final Account account = numberless
          ? generateNumberlessAccount(UUID.randomUUID())
          : generateAccount("+1" + String.format("%03d", i), UUID.randomUUID(), UUID.randomUUID());

      expectedAccountIdentifiers.add(account.getAccountIdentifier());

      if (numberless) {
        createNumberlessAccount(account, receiptPresentation(), TestRandomUtil.nextBytes(16));
      } else {
        createAccount(account);
      }
    }

    @SuppressWarnings("DataFlowIssue") final Set<UUID> retrievedAccountIdentifiers =
        new HashSet<>(accounts.getAllAccountIdentifiers(2, Schedulers.parallel()).collectList().block());

    assertEquals(expectedAccountIdentifiers, retrievedAccountIdentifiers);
  }

  @Test
  void testDelete() {
    final Device deletedDevice = generateDevice(DEVICE_ID_1);
    final Account deletedAccount = generateAccount("+14151112222", UUID.randomUUID(),
        UUID.randomUUID(), List.of(deletedDevice));
    final Device retainedDevice = generateDevice(DEVICE_ID_1);
    final Account retainedAccount = generateAccount("+14151112345", UUID.randomUUID(),
        UUID.randomUUID(), List.of(retainedDevice));

    createAccount(deletedAccount);
    createAccount(retainedAccount);

    assertThat(accounts.findRecentlyDeletedAccountIdentifier(deletedAccount.getPhoneNumberIdentifier().orElseThrow())).isEmpty();

    assertPhoneNumberConstraintExists("+14151112222", deletedAccount.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(deletedAccount.getPhoneNumberIdentifier().orElseThrow(), deletedAccount.getAccountIdentifier());
    assertPhoneNumberConstraintExists("+14151112345", retainedAccount.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(retainedAccount.getPhoneNumberIdentifier().orElseThrow(), retainedAccount.getAccountIdentifier());

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getAccountIdentifier())).isPresent();
    assertThat(accounts.getByAccountIdentifier(retainedAccount.getAccountIdentifier())).isPresent();

    accounts.delete(deletedAccount.getAccountIdentifier(), Collections.emptyList());

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getAccountIdentifier())).isNotPresent();
    assertThat(accounts.findRecentlyDeletedAccountIdentifier(deletedAccount.getPhoneNumberIdentifier().orElseThrow())).hasValue(deletedAccount.getAccountIdentifier());

    assertPhoneNumberConstraintDoesNotExist(deletedAccount.getNumber().orElseThrow());
    assertPhoneNumberIdentifierConstraintDoesNotExist(deletedAccount.getPhoneNumberIdentifier().orElseThrow());

    verifyStoredState(retainedAccount.getNumber(), retainedAccount.getAccountIdentifier(), retainedAccount.getPhoneNumberIdentifier(),
        null, accounts.getByAccountIdentifier(retainedAccount.getAccountIdentifier()).orElseThrow(), retainedAccount);

    {
      final Account recreatedAccount = generateAccount(deletedAccount.getNumber().orElseThrow(), UUID.randomUUID(),
          deletedAccount.getPhoneNumberIdentifier().orElseThrow(), List.of(generateDevice(DEVICE_ID_1)));

      final boolean freshUser = createAccount(recreatedAccount);

      assertThat(freshUser).isTrue();
      assertThat(accounts.getByAccountIdentifier(recreatedAccount.getAccountIdentifier())).isPresent();
      verifyStoredState(recreatedAccount.getNumber(), recreatedAccount.getAccountIdentifier(), recreatedAccount.getPhoneNumberIdentifier(),
          null, accounts.getByAccountIdentifier(recreatedAccount.getAccountIdentifier()).orElseThrow(), recreatedAccount);

      assertPhoneNumberConstraintExists(recreatedAccount.getNumber().orElseThrow(), recreatedAccount.getAccountIdentifier());
      assertPhoneNumberIdentifierConstraintExists(recreatedAccount.getPhoneNumberIdentifier().orElseThrow(), recreatedAccount.getAccountIdentifier());
    }
  }

  @Test
  void testDeleteNumberless() throws InvalidInputException, VerificationFailedException {
    final Account deletedAccount = generateNumberlessAccount(UUID.randomUUID());
    createNumberlessAccount(deletedAccount, receiptPresentation(), TestRandomUtil.nextBytes(16));

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getAccountIdentifier())).isPresent();
    accounts.delete(deletedAccount.getAccountIdentifier(), Collections.emptyList());
    assertThat(accounts.getByAccountIdentifier(deletedAccount.getAccountIdentifier())).isNotPresent();
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void testMissing(@Nullable final String number) throws Exception {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
        List.of(device));

    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptPresentation(), TestRandomUtil.nextBytes(16));
    }

    Optional<Account> retrieved = accounts.getByE164("+11111111");
    assertThat(retrieved).isNotPresent();

    retrieved = accounts.getByAccountIdentifier(UUID.randomUUID());
    assertThat(retrieved).isNotPresent();
  }

  @ParameterizedTest
  @ValueSource(strings = {"+14151112222"})
  @NullSource
  void getByAccountIdentifierAsync(@Nullable final String number) throws Exception {
    assertThat(accounts.getByAccountIdentifierAsync(UUID.randomUUID()).join()).isEmpty();

    final Account account =
        generateAccount(number, UUID.randomUUID(), number == null ? null : UUID.randomUUID(),
            List.of(generateDevice(DEVICE_ID_1)));

    if (number != null) {
      createAccount(account);
    } else {
      createNumberlessAccount(account, receiptPresentation(), TestRandomUtil.nextBytes(16));
    }

    assertThat(accounts.getByAccountIdentifierAsync(account.getAccountIdentifier()).join()).isPresent();
  }

  @Test
  void getByPhoneNumberIdentifierAsync() {
    assertThat(accounts.getByPhoneNumberIdentifierAsync(UUID.randomUUID()).join()).isEmpty();

    final Account account =
        generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

    createAccount(account);

    assertThat(accounts.getByPhoneNumberIdentifierAsync(account.getPhoneNumberIdentifier().orElseThrow()).join()).isPresent();
  }

  @Test
  void getByE164Async() {
    final String e164 = "+14151112222";

    assertThat(accounts.getByE164Async(e164).join()).isEmpty();

    final Account account =
        generateAccount(e164, UUID.randomUUID(), UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

    createAccount(account);

    assertThat(accounts.getByE164Async(e164).join()).isPresent();
  }

  @Test
  void testCanonicallyDiscoverableSet() {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));
    account.setDiscoverableByPhoneNumber(false);
    createAccount(account);
    verifyStoredState(Optional.of("+14151112222"), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, false);
    account.setDiscoverableByPhoneNumber(true);
    accounts.update(account);
    verifyStoredState(Optional.of("+14151112222"), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, true);
    account.setDiscoverableByPhoneNumber(false);
    accounts.update(account);
    verifyStoredState(Optional.of("+14151112222"), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, account, false);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  public void testChangeNumber(final Optional<UUID> maybeDisplacedAccountIdentifier) {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Device device = generateDevice(DEVICE_ID_1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), originalPni, List.of(device));

    createAccount(account);

    assertThat(accounts.getByPhoneNumberIdentifier(originalPni)).isPresent();

    assertPhoneNumberConstraintExists(originalNumber, account.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getAccountIdentifier());

    {
      final Optional<Account> retrieved = accounts.getByE164(originalNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(Optional.of(originalNumber), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);
    }

    accounts.changeNumber(account, targetNumber, targetPni, maybeDisplacedAccountIdentifier, Collections.emptyList());

    assertThat(accounts.getByE164(originalNumber)).isEmpty();
    assertThat(accounts.getByAccountIdentifier(originalPni)).isEmpty();

    assertPhoneNumberConstraintDoesNotExist(originalNumber);
    assertPhoneNumberIdentifierConstraintDoesNotExist(originalPni);
    assertPhoneNumberConstraintExists(targetNumber, account.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(targetPni, account.getAccountIdentifier());

    {
      final Optional<Account> retrieved = accounts.getByE164(targetNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(Optional.of(targetNumber), account.getAccountIdentifier(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);

      assertThat(retrieved.get().getPhoneNumberIdentifier()).hasValue(targetPni);
      assertThat(accounts.getByPhoneNumberIdentifier(targetPni)).isPresent();
    }

    assertThat(accounts.findRecentlyDeletedAccountIdentifier(originalPni)).isEqualTo(maybeDisplacedAccountIdentifier);
  }

  private static Stream<Arguments> testChangeNumber() {
    return Stream.of(
        Arguments.of(Optional.empty()),
        Arguments.of(Optional.of(UUID.randomUUID()))
    );
  }

  @Test
  public void testChangeNumberConflict() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Device existingDevice = generateDevice(DEVICE_ID_1);
    final Account existingAccount = generateAccount(targetNumber, UUID.randomUUID(), targetPni, List.of(existingDevice));

    final Device device = generateDevice(DEVICE_ID_1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), originalPni, List.of(device));

    createAccount(account);
    createAccount(existingAccount);

    assertThrows(TransactionCanceledException.class, () -> accounts.changeNumber(account, targetNumber, targetPni, Optional.of(existingAccount.getAccountIdentifier()), Collections.emptyList()));

    assertPhoneNumberConstraintExists(originalNumber, account.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getAccountIdentifier());
    assertPhoneNumberConstraintExists(targetNumber, existingAccount.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(targetPni, existingAccount.getAccountIdentifier());
  }

  @Test
  public void testChangeNumberPhoneNumberIdentifierConflict() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final Device device = generateDevice(DEVICE_ID_1);
    final Account account = generateAccount(originalNumber, UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    createAccount(account);

    final UUID existingAccountIdentifier = UUID.randomUUID();
    final UUID existingPhoneNumberIdentifier = UUID.randomUUID();

    // Artificially inject a conflicting PNI entry
    DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
        .tableName(Tables.PNI_ASSIGNMENTS.tableName())
        .item(Map.of(
            Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(existingPhoneNumberIdentifier),
            Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(existingAccountIdentifier)))
        .conditionExpression(
            "attribute_not_exists(#pni) OR (attribute_exists(#pni) AND #uuid = :uuid)")
        .expressionAttributeNames(
            Map.of("#uuid", Accounts.KEY_ACCOUNT_UUID,
                "#pni", Accounts.ATTR_PNI_UUID))
        .expressionAttributeValues(
            Map.of(":uuid", AttributeValues.fromUUID(existingAccountIdentifier)))
        .build());

    assertThrows(TransactionCanceledException.class, () -> accounts.changeNumber(account, targetNumber, existingPhoneNumberIdentifier, Optional.empty(), Collections.emptyList()));
  }

  @Test
  public void testChangeNumberContestedOptimisticLock() {
    final String originalNumber = "+14151112222";
    final String targetNumber = "+14151113333";

    final UUID originalPni = UUID.randomUUID();
    final UUID targetPni = UUID.randomUUID();

    final Device device = generateDevice(DEVICE_ID_1);
    final Account firstAccountInstance = generateAccount(originalNumber, UUID.randomUUID(), originalPni,
        List.of(device));

    createAccount(firstAccountInstance);

    final Account secondAccountInstance = accounts.getByAccountIdentifier(firstAccountInstance.getAccountIdentifier()).orElseThrow();

    // update via the first instance, which will update the version
    firstAccountInstance.setCurrentProfileVersion(new byte[32]);
    accounts.update(firstAccountInstance);

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.changeNumber(secondAccountInstance, targetNumber, targetPni, Optional.empty(),
            Collections.emptyList()), "Second account instance has stale version");

    final Account refreshedAccountInstance = accounts.getByAccountIdentifier(firstAccountInstance.getAccountIdentifier())
        .orElseThrow();
    accounts.changeNumber(refreshedAccountInstance, targetNumber, targetPni, Optional.empty(),
        Collections.emptyList());

    assertPhoneNumberConstraintDoesNotExist(originalNumber);
    assertPhoneNumberIdentifierConstraintDoesNotExist(originalPni);
    assertPhoneNumberConstraintExists(targetNumber, firstAccountInstance.getAccountIdentifier());
    assertPhoneNumberIdentifierConstraintExists(targetPni, firstAccountInstance.getAccountIdentifier());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSwitchUsernameHashes(final boolean numberless) throws UsernameHashNotAvailableException {
    final Account account = createAccount(numberless);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    final UUID oldHandle = account.getUsernameLinkHandle();

    {
      final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_1).join();
      assertThat(maybeAccount.orElseThrow().getUsernameHash()).hasValue(USERNAME_HASH_1);
      verifyAccountEquals(maybeAccount.orElseThrow(), account);

      final Optional<Account> maybeAccount2 = accounts.getByUsernameLinkHandle(oldHandle).join();
      assertThat(maybeAccount2.orElseThrow().getUsernameHash()).hasValue(USERNAME_HASH_1);
      verifyAccountEquals(maybeAccount2.orElseThrow(), account);
    }

    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_2);
    final UUID newHandle = account.getUsernameLinkHandle();

    // switching usernames should put a hold on our original username
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).containsExactlyInAnyOrderEntriesOf(Map.of(
        Accounts.UsernameTable.KEY_USERNAME_HASH, AttributeValues.b(USERNAME_HASH_1),
        Accounts.UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.b(account.getAccountIdentifier()),
        Accounts.UsernameTable.ATTR_CONFIRMED, AttributeValues.fromBool(false),
        Accounts.UsernameTable.ATTR_TTL,
        AttributeValues.n(clock.instant().plus(Accounts.USERNAME_HOLD_DURATION).getEpochSecond())));
    assertThat(accounts.getByUsernameLinkHandle(oldHandle).join()).isEmpty();

    {
      final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_2).join();

      assertThat(maybeAccount).isPresent();
      assertThat(maybeAccount.orElseThrow().getUsernameHash()).hasValue(USERNAME_HASH_2);
      verifyAccountEquals(maybeAccount.orElseThrow(), account);
      final Optional<Account> maybeAccount2 = accounts.getByUsernameLinkHandle(newHandle).join();
      assertThat(maybeAccount2.orElseThrow().getUsernameHash()).hasValue(USERNAME_HASH_2);
      verifyAccountEquals(maybeAccount2.orElseThrow(), account);
    }
  }

  @Test
  void testUsernameHashNotAvailable() {
    final Account firstAccount = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    final Account secondAccount = generateAccount("+18005559876", UUID.randomUUID(), UUID.randomUUID());

    createAccount(firstAccount);
    createAccount(secondAccount);

    // first account reserves and confirms username hash
    assertThatNoException().isThrownBy(() -> {
      accounts.reserveUsernameHash(firstAccount, USERNAME_HASH_1, Duration.ofDays(1));
      accounts.confirmUsernameHash(firstAccount, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    });

    final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_1).join();

    assertThat(maybeAccount).isPresent();
    verifyStoredState(firstAccount.getNumber(), firstAccount.getAccountIdentifier(), firstAccount.getPhoneNumberIdentifier(), USERNAME_HASH_1, maybeAccount.get(), firstAccount);

    // throw an error if second account tries to reserve or confirm the same username hash
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.reserveUsernameHash(secondAccount, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.confirmUsernameHash(secondAccount, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));

    // throw an error if first account tries to reserve or confirm the username hash that it has already confirmed
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.reserveUsernameHash(firstAccount, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.confirmUsernameHash(firstAccount, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));

    assertThat(secondAccount.getReservedUsernameHash()).isEmpty();
    assertThat(secondAccount.getUsernameHash()).isEmpty();
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void testReserveUsernameHashTransactionConflict(final Optional<String> constraintCancellationString,
      final Optional<String> accountsCancellationString,
      final Class<Exception> expectedException) {
    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    accounts = new Accounts(
        clock,
        dynamoDbClient,
        mock(DynamoDbAsyncClient.class),
        new RedeemedReceiptsManager(clock, Tables.REDEEMED_RECEIPTS.tableName(),
            dynamoDbClient),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());
    final Account account = generateAccount("+14155551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    final CancellationReason constraintCancellationReason = constraintCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    final CancellationReason accountsCancellationReason = accountsCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .cancellationReasons(constraintCancellationReason, accountsCancellationReason)
            .build());

    assertThrows(expectedException,
        () -> accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
  }

  private static Stream<Arguments> testReserveUsernameHashTransactionConflict() {
    return Stream.of(
        Arguments.of(Optional.of("TransactionConflict"), Optional.empty(), ContestedOptimisticLockException.class),
        Arguments.of(Optional.empty(), Optional.of("TransactionConflict"), ContestedOptimisticLockException.class),
        Arguments.of(Optional.of("ConditionalCheckFailed"), Optional.of("TransactionConflict"), UsernameHashNotAvailableException.class)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void testConfirmUsernameHashTransactionConflict(final Optional<String> constraintCancellationString,
      final Optional<String> accountsCancellationString,
      final Class<Exception> expectedException) {
    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    accounts = new Accounts(
        clock,
        dynamoDbClient,
        mock(DynamoDbAsyncClient.class),
        new RedeemedReceiptsManager(clock, Tables.REDEEMED_RECEIPTS.tableName(),
            dynamoDbClient),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());
    final Account account = generateAccount("+14155551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    final CancellationReason constraintCancellationReason = constraintCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    final CancellationReason accountsCancellationReason = accountsCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .cancellationReasons(constraintCancellationReason,
                accountsCancellationReason,
                CancellationReason.builder().build())
            .build());

    assertThrows(expectedException,
        () -> accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
  }

  private static Stream<Arguments> testConfirmUsernameHashTransactionConflict() {
    return Stream.of(
        Arguments.of(Optional.of("TransactionConflict"), Optional.empty(), ContestedOptimisticLockException.class),
        Arguments.of(Optional.empty(), Optional.of("TransactionConflict"), ContestedOptimisticLockException.class),
        Arguments.of(Optional.of("ConditionalCheckFailed"), Optional.of("TransactionConflict"), UsernameHashNotAvailableException.class)
    );
  }

  @Test
  void testConfirmUsernameHashVersionMismatch() throws UsernameHashNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    account.setVersion(account.getVersion() + 77);

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));

    assertThat(account.getUsernameHash()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testClearUsername(final boolean numberless) throws UsernameHashNotAvailableException {
    final Account account = createAccount(numberless);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isPresent();

    accounts.clearUsernameHash(account);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(accounts.getByAccountIdentifier(account.getAccountIdentifier()))
        .hasValueSatisfying(clearedAccount -> {
          assertThat(clearedAccount.getUsernameHash()).isEmpty();
          assertThat(clearedAccount.getUsernameLinkHandle()).isNull();
          assertThat(clearedAccount.getEncryptedUsername()).isEmpty();
        });
  }

  @Test
  void testClearUsernameNoUsername() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    assertThatNoException().isThrownBy(() -> accounts.clearUsernameHash(account));
  }

  @Test
  void testClearUsernameVersionMismatch() throws UsernameHashNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    account.setVersion(account.getVersion() + 12);

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.clearUsernameHash(account));

    assertArrayEquals(USERNAME_HASH_1, account.getUsernameHash().orElseThrow());
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void testClearUsernameTransactionConflict(final Optional<String> constraintCancellationString,
      final Optional<String> accountsCancellationString) throws UsernameHashNotAvailableException {
    final DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    accounts = new Accounts(
        clock,
        dynamoDbClient,
        mock(DynamoDbAsyncClient.class),
        new RedeemedReceiptsManager(clock, Tables.REDEEMED_RECEIPTS.tableName(),
            dynamoDbClient),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName(),
        Tables.USED_LINK_DEVICE_TOKENS.tableName());

    final Account account = generateAccount("+14155551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(mock(TransactWriteItemsResponse.class));

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    final CancellationReason constraintCancellationReason = constraintCancellationString
        .map(reason -> CancellationReason.builder().code(reason).build())
        .orElse(CancellationReason.builder().build());

    final CancellationReason accountsCancellationReason = accountsCancellationString
        .map(reason -> CancellationReason.builder().code(reason).build())
        .orElse(CancellationReason.builder().build());

    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .cancellationReasons(accountsCancellationReason, constraintCancellationReason)
            .build());

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.clearUsernameHash(account));

    assertArrayEquals(USERNAME_HASH_1, account.getUsernameHash().orElseThrow());
  }

  private static Stream<Arguments> testClearUsernameTransactionConflict() {
    return Stream.of(
        Arguments.of(Optional.empty(), Optional.of("TransactionConflict"), ContestedOptimisticLockException.class),
        Arguments.of(Optional.of("TransactionConflict"), Optional.empty(), ContestedOptimisticLockException.class)
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testReservedUsernameHash(final boolean numberless) throws UsernameHashNotAvailableException {
    final Account account1 = createAccount(numberless);
    final Account account2 = createAccount(numberless);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(USERNAME_HASH_1, account1.getReservedUsernameHash().orElseThrow());
    assertThat(account1.getUsernameHash()).isEmpty();

    // account 2 shouldn't be able to reserve or confirm the same username hash
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    accounts.confirmUsernameHash(account1, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    assertThat(account1.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(USERNAME_HASH_1, account1.getUsernameHash().orElseThrow());
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join().orElseThrow().getAccountIdentifier()).isEqualTo(account1.getAccountIdentifier());

    final Map<String, AttributeValue> usernameConstraintRecord = getUsernameConstraintTableItem(USERNAME_HASH_1);

    assertThat(usernameConstraintRecord).containsKey(Accounts.UsernameTable.KEY_USERNAME_HASH);
    assertThat(usernameConstraintRecord).doesNotContainKey(Accounts.UsernameTable.ATTR_TTL);
  }

  @Test
  void switchBetweenReservedUsernameHashes() throws UsernameHashNotAvailableException {
    final Account account = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(USERNAME_HASH_1, account.getReservedUsernameHash().orElseThrow());
    assertThat(account.getUsernameHash()).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofDays(1));
    assertArrayEquals(USERNAME_HASH_2, account.getReservedUsernameHash().orElseThrow());
    assertThat(account.getUsernameHash()).isEmpty();

    final Map<String, AttributeValue> usernameConstraintRecord1 = getUsernameConstraintTableItem(USERNAME_HASH_1);
    final Map<String, AttributeValue> usernameConstraintRecord2 = getUsernameConstraintTableItem(USERNAME_HASH_2);
    assertThat(usernameConstraintRecord1).containsKey(Accounts.UsernameTable.KEY_USERNAME_HASH);
    assertThat(usernameConstraintRecord2).containsKey(Accounts.UsernameTable.KEY_USERNAME_HASH);
    assertThat(usernameConstraintRecord1).containsKey(Accounts.UsernameTable.ATTR_TTL);
    assertThat(usernameConstraintRecord2).containsKey(Accounts.UsernameTable.ATTR_TTL);

    clock.pin(Instant.EPOCH.plus(Duration.ofMinutes(1)));

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(USERNAME_HASH_1, account.getReservedUsernameHash().orElseThrow());
    assertThat(account.getUsernameHash()).isEmpty();

    final Map<String, AttributeValue> newUsernameConstraintRecord1 = getUsernameConstraintTableItem(USERNAME_HASH_1);
    assertThat(newUsernameConstraintRecord1).containsKey(Accounts.UsernameTable.KEY_USERNAME_HASH);
    assertThat(newUsernameConstraintRecord1).containsKey(Accounts.UsernameTable.ATTR_TTL);
    assertThat(usernameConstraintRecord1.get(Accounts.UsernameTable.ATTR_TTL))
        .isNotEqualTo(newUsernameConstraintRecord1.get(Accounts.UsernameTable.ATTR_TTL));
  }

  @Test
  void reserveOwnConfirmedUsername() throws UsernameHashNotAvailableException {
    final Account account = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(USERNAME_HASH_1, account.getReservedUsernameHash().orElseThrow());
    assertThat(account.getUsernameHash()).isEmpty();
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).containsKey(Accounts.UsernameTable.ATTR_TTL);


    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(USERNAME_HASH_1, account.getUsernameHash().orElseThrow());
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).doesNotContainKey(Accounts.UsernameTable.ATTR_TTL);

    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(USERNAME_HASH_1, account.getUsernameHash().orElseThrow());
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).containsKey(Accounts.UsernameTable.KEY_USERNAME_HASH);
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).doesNotContainKey(Accounts.UsernameTable.ATTR_TTL);
  }

  @Test
  void testConfirmReservedUsernameHashWrongAccountUuid() throws UsernameHashNotAvailableException {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1));
    assertArrayEquals(USERNAME_HASH_1, account1.getReservedUsernameHash().orElseThrow());
    assertThat(account1.getUsernameHash()).isEmpty();

    // only account1 should be able to confirm the reserved hash
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmExpiredReservedUsernameHash() throws UsernameHashNotAvailableException {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(2));

    for (int i = 0; i <= 2; i++) {
      clock.pin(Instant.EPOCH.plus(Duration.ofDays(i)));
      assertThrows(UsernameHashNotAvailableException.class,
          () -> accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)));
    }

    // after 2 days, can reserve and confirm the hash
    clock.pin(Instant.EPOCH.plus(Duration.ofDays(2)).plus(Duration.ofSeconds(1)));
    accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1));
    assertEquals(USERNAME_HASH_1, account2.getReservedUsernameHash().orElseThrow());

    accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(2)));
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.confirmUsernameHash(account1, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join().orElseThrow().getAccountIdentifier()).isEqualTo(account2.getAccountIdentifier());
  }

  @Test
  void testReserveConfirmUsernameHashVersionConflict() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    account.setVersion(account.getVersion() + 12);
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertThat(account.getUsernameHash()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRemoveOldestHold(boolean clearUsername) throws UsernameHashNotAvailableException {
    Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    final List<byte[]> usernames = IntStream.range(0, 7).mapToObj(_ -> TestRandomUtil.nextBytes(32)).toList();
    final ArrayDeque<byte[]> expectedHolds = new ArrayDeque<>();
    expectedHolds.add(USERNAME_HASH_1);

    for (byte[] username : usernames) {
      accounts.reserveUsernameHash(account, username, Duration.ofDays(1));
      accounts.confirmUsernameHash(account, username, ENCRYPTED_USERNAME_1);
      assertThat(accounts.getByUsernameHash(username).join()).isPresent();

      final Account read = accounts.getByAccountIdentifier(account.getAccountIdentifier()).orElseThrow();
      assertThat(read.getUsernameHolds().stream().map(Account.UsernameHold::usernameHash).toList())
          .containsExactlyElementsOf(expectedHolds);

      expectedHolds.add(username);
      if (expectedHolds.size() == Accounts.MAX_USERNAME_HOLDS + 1) {
        expectedHolds.pop();
      }

      // clearing the username adds a hold, but the subsequent confirm in the next iteration should add the same hold
      // (should be a noop) so we don't need to touch expectedHolds
      if (clearUsername) {
        accounts.clearUsernameHash(account);
      }
    }


    final Account account2 = generateAccount("+18005554321", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);

    // someone else should be able to get any of the usernames except the held usernames (MAX_HOLDS) +1 for the username
    // currently held by the other account if we didn't clear it
    final int numFree = usernames.size() - Accounts.MAX_USERNAME_HOLDS - (clearUsername ? 0 : 1);
    final List<byte[]> freeUsernames = usernames.subList(0, numFree);
    final List<byte[]> heldUsernames = usernames.subList(numFree, usernames.size());
    for (byte[] username : freeUsernames) {
      assertDoesNotThrow(() -> accounts.reserveUsernameHash(account2, username, Duration.ofDays(2)));
    }
    for (byte[] username : heldUsernames) {
      assertThrows(UsernameHashNotAvailableException.class,
          () -> accounts.reserveUsernameHash(account2, username, Duration.ofDays(2)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHoldUsername(final boolean numberless) throws UsernameHashNotAvailableException {
    final Account account = createAccount(numberless);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    accounts.clearUsernameHash(account);

    Account account2 = generateAccount("+18005554321", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);
    assertThrows(UsernameHashNotAvailableException.class,
        () -> accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)),
        "account2 should not be able reserve username held by account");

    // but we should be able to get it back
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
  }

  @Test
  void testNoHoldsBarred() throws UsernameHashNotAvailableException {
    // should be able to reserve all MAX_HOLDS usernames
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    final List<byte[]> usernames = IntStream.range(0, Accounts.MAX_USERNAME_HOLDS + 1)
        .mapToObj(_ -> TestRandomUtil.nextBytes(32))
        .toList();
    for (byte[] username : usernames) {
      accounts.reserveUsernameHash(account, username, Duration.ofDays(1));
      accounts.confirmUsernameHash(account, username, ENCRYPTED_USERNAME_1);
    }

    // someone else shouldn't be able to get any of our holds
    Account account2 = generateAccount("+18005554321", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);
    for (byte[] username : usernames) {
      assertThrows(UsernameHashNotAvailableException.class,
          () -> accounts.reserveUsernameHash(account2, username, Duration.ofDays(1)),
          "account2 should not be able reserve username held by account");
    }

    // once the hold expires it's fine though
    clock.pin(Instant.EPOCH.plus(Accounts.USERNAME_HOLD_DURATION).plus(Duration.ofSeconds(1)));
    accounts.reserveUsernameHash(account2, usernames.getFirst(), Duration.ofDays(1));

    // if account1 modifies their username, we should also clear out the old holds, leaving only their newly added hold
    accounts.clearUsernameHash(account);
    assertThat(account.getUsernameHolds().stream().map(Account.UsernameHold::usernameHash))
        .containsExactly(usernames.getLast());
  }

  @Test
  public void testCannotRemoveHold() throws UsernameHashNotAvailableException {
    // Tests the case where we are trying to remove a hold we think we have, but it turns out we've already lost it.
    // This means that the Account record an account has a hold on a particular username, but that hold is held by
    // someone else in the username table. This can happen when the hold TTL expires while we are performing the update
    // operation that attempts to remove the hold, and another user swoops in and takes the held username. In this
    // case, a simple retry should let us check the clock again and notice that our hold in our account has expired.
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_1);

    // Now we have a hold on username_hash_1. Simulate a race where the TTL on username_hash_1 expires, and someone
    // else picks up the username by going forward and then back in time
    Account account2 = generateAccount("+18005554321", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);
    clock.pin(Instant.EPOCH.plus(Accounts.USERNAME_HOLD_DURATION).plus(Duration.ofSeconds(1)));
    accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    clock.pin(Instant.EPOCH);
    // already have 1 hold, should be able to get to MAX_HOLDS without a problem
    for (int i = 1; i < Accounts.MAX_USERNAME_HOLDS; i++) {
      accounts.reserveUsernameHash(account, TestRandomUtil.nextBytes(32), Duration.ofDays(1));
      accounts.confirmUsernameHash(account, TestRandomUtil.nextBytes(32), ENCRYPTED_USERNAME_1);
    }

    accounts.reserveUsernameHash(account, TestRandomUtil.nextBytes(32), Duration.ofDays(1));
    // Should fail, because we cannot remove our hold on USERNAME_HASH_1
    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.confirmUsernameHash(account, TestRandomUtil.nextBytes(32), ENCRYPTED_USERNAME_1));

    // Should now pass once we realize our hold's TTL is over
    clock.pin(Instant.EPOCH.plus(Accounts.USERNAME_HOLD_DURATION).plus(Duration.ofSeconds(1)));
    accounts.confirmUsernameHash(account, TestRandomUtil.nextBytes(32), ENCRYPTED_USERNAME_1);
  }

  @Test
  void testDeduplicateHoldsOnSwappedUsernames() throws UsernameHashNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    final Consumer<byte[]> assertSingleHold = (byte[] usernameToCheck) -> {
      // our account should have exactly one hold for the username
      assertThat(account.getUsernameHolds().stream().map(Account.UsernameHold::usernameHash).toList())
          .containsExactly(usernameToCheck);

      // the username should be reserved for USERNAME_HOLD_DURATION (a re-reservation shouldn't reduce our expiration to
      // the provided reservation TTL)
      assertThat(
          AttributeValues.getLong(getUsernameConstraintTableItem(usernameToCheck), Accounts.UsernameTable.ATTR_TTL, 0L))
          .isEqualTo(Accounts.USERNAME_HOLD_DURATION.getSeconds());
    };

    // Swap back and forth between username 1 and 2.  Username hashes shouldn't reappear in our holds if we already have
    // a hold
    for (int i = 0; i < 5; i++) {
      accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofSeconds(1));
      accounts.confirmUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_1);
      assertSingleHold.accept(USERNAME_HASH_1);

      accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofSeconds(1));
      accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);
      assertSingleHold.accept(USERNAME_HASH_2);
    }
  }

  @Test
  void testRemoveHoldAfterConfirm() throws UsernameHashNotAvailableException {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    final List<byte[]> usernames = IntStream.range(0, Accounts.MAX_USERNAME_HOLDS)
        .mapToObj(_ -> TestRandomUtil.nextBytes(32)).toList();
    for (byte[] username : usernames) {
      accounts.reserveUsernameHash(account, username, Duration.ofDays(1));
      accounts.confirmUsernameHash(account, username, ENCRYPTED_USERNAME_1);
    }

    int holdToRereserve = (Accounts.MAX_USERNAME_HOLDS / 2) - 1;

    // should have MAX_HOLDS - 1 holds (everything in usernames except the last username, which is our current)
    assertThat(account.getUsernameHolds().stream().map(Account.UsernameHold::usernameHash).toList())
        .containsExactlyElementsOf(usernames.subList(0, usernames.size() - 1));

    // if we confirm a username we already have held, it should just drop out of the holds list
    accounts.reserveUsernameHash(account, usernames.get(holdToRereserve), Duration.ofDays(1));
    accounts.confirmUsernameHash(account, usernames.get(holdToRereserve), ENCRYPTED_USERNAME_1);

    // should have a hold on every username but the one we just confirmed
    assertThat(account.getUsernameHolds().stream().map(Account.UsernameHold::usernameHash).toList())
        .containsExactlyElementsOf(Stream.concat(
                usernames.subList(0, holdToRereserve).stream(),
                usernames.subList(holdToRereserve + 1, usernames.size()).stream())
            .toList());
  }


  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIgnoredFieldsNotAddedToDataAttribute(final boolean numberless) throws Exception {
    final Account account = numberless
        ? generateNumberlessAccount(UUID.randomUUID())
        : generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    account.setUsernameHash(TestRandomUtil.nextBytes(32));
    account.setUsernameLinkDetails(UUID.randomUUID(), TestRandomUtil.nextBytes(32));
    if (numberless) {
      createNumberlessAccount(account, receiptPresentation(), TestRandomUtil.nextBytes(16));
    } else {
      createAccount(account);
    }
    final Map<String, AttributeValue> accountRecord = DYNAMO_DB_EXTENSION.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(Tables.ACCOUNTS.tableName())
            .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getAccountIdentifier())))
            .build())
        .item();
    final Map<?, ?> dataMap = SystemMapper.jsonMapper()
        .readValue(accountRecord.get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), Map.class);
    Accounts.ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION
        .forEach(field -> assertFalse(dataMap.containsKey(field)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testGetByUsernameHashAsync(final boolean numberless) throws UsernameHashNotAvailableException {
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    final Account account = createAccount(numberless);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1));
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isPresent();
  }

  @Test
  void testInvalidDeviceIdDeserialization() throws Exception {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    final Device device2 = generateDevice((byte) 64);
    account.addDevice(device2);

    createAccount(account);

    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().getItem(GetItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getAccountIdentifier())))
        .build()).join();

    final Map<?, ?> accountData = SystemMapper.jsonMapper()
        .readValue(response.item().get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), Map.class);

    @SuppressWarnings("unchecked") final List<Map<Object, Object>> devices =
        (List<Map<Object, Object>>) accountData.get("devices");

    assertEquals((int) device2.getId(), devices.get(1).get("id"));

    devices.get(1).put("id", Byte.MAX_VALUE + 5);

    DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().updateItem(UpdateItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getAccountIdentifier())))
        .updateExpression("SET #data = :data")
        .expressionAttributeNames(Map.of("#data", Accounts.ATTR_ACCOUNT_DATA))
        .expressionAttributeValues(
            Map.of(":data", AttributeValues.fromByteArray(SystemMapper.jsonMapper().writeValueAsBytes(accountData))))
        .build()).join();

    final CompletionException e = assertThrows(CompletionException.class,
        () -> accounts.getByAccountIdentifierAsync(account.getAccountIdentifier()).join());

    Throwable cause = e.getCause();
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }

    assertInstanceOf(DeviceIdDeserializer.DeviceIdDeserializationException.class, cause);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRegenerateConstraints(final boolean numberless) {
    final Instant usernameHoldExpiration = clock.instant().plus(Accounts.USERNAME_HOLD_DURATION).truncatedTo(ChronoUnit.SECONDS);

    final Account account = numberless ? generateNumberlessAccount(UUID.randomUUID()) : nextRandomAccount();
    account.setUsernameHash(USERNAME_HASH_1);
    account.setUsernameLinkDetails(UUID.randomUUID(), ENCRYPTED_USERNAME_1);
    account.setUsernameHolds(List.of(new Account.UsernameHold(USERNAME_HASH_2, usernameHoldExpiration.getEpochSecond())));

    writeAccountRecordWithoutConstraints(account);
    accounts.regenerateConstraints(account).join();

    // Check that constraints do what they should from a functional perspective
    if (!numberless) {
      final Account conflictingNumberAccount = nextRandomAccount();
      conflictingNumberAccount.setNumber(account.getNumber().orElseThrow(), account.getAccountIdentifier());

      assertThrows(AccountAlreadyExistsException.class,
          () -> accounts.create(conflictingNumberAccount, Collections.emptyList()));
    }

    {
      final Account conflictingUsernameAccount = nextRandomAccount();
      createAccount(conflictingUsernameAccount);

      assertThrows(UsernameHashNotAvailableException.class,
          () -> accounts.reserveUsernameHash(conflictingUsernameAccount, USERNAME_HASH_1, Accounts.USERNAME_HOLD_DURATION));
    }

    {
      final Account conflictingUsernameHoldAccount = nextRandomAccount();
      createAccount(conflictingUsernameHoldAccount);

      assertThrows(UsernameHashNotAvailableException.class,
          () -> accounts.reserveUsernameHash(conflictingUsernameHoldAccount, USERNAME_HASH_2, Accounts.USERNAME_HOLD_DURATION));
    }

    // Check that bare constraint records are written as expected
    if (!numberless) {
      assertEquals(Optional.of(account.getAccountIdentifier()),
          getConstraintValue(Tables.NUMBERS.tableName(), Accounts.ATTR_ACCOUNT_E164,
              AttributeValues.fromString(account.getNumber().get())));

      assertEquals(Optional.of(account.getAccountIdentifier()),
          getConstraintValue(Tables.PNI_ASSIGNMENTS.tableName(), Accounts.ATTR_PNI_UUID,
              AttributeValues.fromUUID(account.getPhoneNumberIdentifier().orElseThrow())));
    }

    assertEquals(Optional.of(new UsernameConstraint(account.getAccountIdentifier(), true, Optional.empty())),
        getUsernameConstraint(USERNAME_HASH_1));

    assertEquals(Optional.of(new UsernameConstraint(account.getAccountIdentifier(), false, Optional.of(usernameHoldExpiration))),
        getUsernameConstraint(USERNAME_HASH_2));
  }

  @Test
  void testRegeneratedConstraintsMatchOriginalConstraints() throws UsernameHashNotAvailableException {
    final Instant usernameHoldExpiration = clock.instant().plus(Accounts.USERNAME_HOLD_DURATION).truncatedTo(ChronoUnit.SECONDS);

    final Account account = nextRandomAccount();
    account.setUsernameHash(USERNAME_HASH_1);
    account.setUsernameLinkDetails(UUID.randomUUID(), ENCRYPTED_USERNAME_1);
    account.setUsernameHolds(List.of(new Account.UsernameHold(USERNAME_HASH_2, usernameHoldExpiration.getEpochSecond())));

    createAccount(account);
    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Accounts.USERNAME_HOLD_DURATION);
    accounts.confirmUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_2);
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Accounts.USERNAME_HOLD_DURATION);
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1);

    final Map<String, AttributeValue> originalE164ConstraintItem =
        DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
                .tableName(Tables.NUMBERS.tableName())
                .key(Map.of(Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber().orElseThrow())))
                .build())
            .item();

    final Map<String, AttributeValue> originalPniConstraintItem =
        DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
                .tableName(Tables.PNI_ASSIGNMENTS.tableName())
                .key(Map.of(Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(account.getPhoneNumberIdentifier().orElseThrow())))
                .build())
            .item();

    final Set<Map<String, AttributeValue>> originalUsernameConstraints = new HashSet<>(
        DYNAMO_DB_EXTENSION.getDynamoDbClient().scan(ScanRequest.builder()
                .tableName(Tables.USERNAMES.tableName())
                .build())
            .items());

    accounts.delete(account.getAccountIdentifier(), Collections.emptyList());

    writeAccountRecordWithoutConstraints(account);
    accounts.regenerateConstraints(account).join();

    final Map<String, AttributeValue> regeneratedE164ConstraintItem =
        DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
                .tableName(Tables.NUMBERS.tableName())
                .key(Map.of(Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber().orElseThrow())))
                .build())
            .item();

    final Map<String, AttributeValue> regeneratedPniConstraintItem =
        DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
                .tableName(Tables.PNI_ASSIGNMENTS.tableName())
                .key(Map.of(Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(account.getPhoneNumberIdentifier().orElseThrow())))
                .build())
            .item();

    final Set<Map<String, AttributeValue>> regeneratedUsernameConstraints = new HashSet<>(
        DYNAMO_DB_EXTENSION.getDynamoDbClient().scan(ScanRequest.builder()
                .tableName(Tables.USERNAMES.tableName())
                .build())
            .items());

    assertEquals(originalE164ConstraintItem, regeneratedE164ConstraintItem);
    assertEquals(originalPniConstraintItem, regeneratedPniConstraintItem);
    assertEquals(originalUsernameConstraints, regeneratedUsernameConstraints);
  }

  private void writeAccountRecordWithoutConstraints(final Account account) {
    final AttributeValue accountData;

    try {
      accountData = AttributeValues.fromByteArray(Accounts.ACCOUNT_DDB_JSON_WRITER.writeValueAsBytes(account));
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getAccountIdentifier()),
        Accounts.ATTR_ACCOUNT_DATA, accountData,
        Accounts.ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
        Accounts.ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.isDiscoverableByPhoneNumber())));

    account.getNumber()
        .map(AttributeValues::fromString)
        .ifPresent(number -> item.put(Accounts.ATTR_ACCOUNT_E164, number));

    account.getPhoneNumberIdentifier()
        .map(AttributeValues::fromUUID)
        .ifPresent(pni -> item.put(Accounts.ATTR_PNI_UUID, pni));

    account.getUnidentifiedAccessKey()
        .map(AttributeValues::fromByteArray)
        .ifPresent(uak -> item.put(Accounts.ATTR_UAK, uak));

    DYNAMO_DB_EXTENSION.getDynamoDbClient().putItem(PutItemRequest.builder()
            .tableName(Tables.ACCOUNTS.tableName())
            .item(item)
        .build());
  }

  private Optional<UUID> getConstraintValue(final String tableName,
      final String keyName,
      final AttributeValue keyValue) {

    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(keyName, keyValue))
        .build());

    return response.hasItem()
        ? Optional.ofNullable(AttributeValues.getUUID(response.item(), Accounts.KEY_ACCOUNT_UUID, null))
        : Optional.empty();
  }

  private Optional<UsernameConstraint> getUsernameConstraint(final byte[] usernameHash) {
    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(Tables.USERNAMES.tableName())
            .key(Map.of(Accounts.UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash)))
        .build());

    if (response.hasItem()) {
      final UUID accountIdentifier =
          AttributeValues.getUUID(response.item(), Accounts.UsernameTable.ATTR_ACCOUNT_UUID, null);

      final boolean confirmed = AttributeValues.getBool(response.item(), Accounts.UsernameTable.ATTR_CONFIRMED, false);

      final Optional<Instant> expiration = response.item().containsKey(Accounts.UsernameTable.ATTR_TTL)
          ? Optional.of(Instant.ofEpochSecond(AttributeValues.getLong(response.item(), Accounts.UsernameTable.ATTR_TTL, 0)))
          : Optional.empty();

      return Optional.of(new UsernameConstraint(accountIdentifier, confirmed, expiration));
    }

    return Optional.empty();
  }

  private static Device generateDevice(byte id) {
    return DevicesHelper.createDevice(id);
  }

  private boolean createAccount(final Account account) {
    try {
      return accounts.create(account, Collections.emptyList());
    } catch (AccountAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }

  private boolean createNumberlessAccount(final Account account, final ReceiptCredentialPresentation receiptCredentialPresentation, final byte[] accountRecoveryPassword) {
    try {
      return accounts.create(account, receiptCredentialPresentation, accountRecoveryPassword, Collections.emptyList());
    } catch (final AccountAlreadyExistsException | ReceiptAlreadyRedeemedException e) {
      throw new IllegalStateException(e);
    }
  }

  /// Generate a random account and create it
  private Account createAccount(final boolean numberless) {
    try {
      final Account account;
      if (numberless) {
        account = generateNumberlessAccount(UUID.randomUUID());
        createNumberlessAccount(account, receiptPresentation(), TestRandomUtil.nextBytes(16));
      } else {
        account = nextRandomAccount();
        createAccount(account);
      }
      return account;
    } catch (InvalidInputException | VerificationFailedException e) {
      throw new AssertionError(e);
    }
  }

  private static Account nextRandomAccount() {
    final String nextNumber = "+1800%07d".formatted(ACCOUNT_COUNTER.getAndIncrement());
    return generateAccount(nextNumber, UUID.randomUUID(), UUID.randomUUID());
  }

  private static Account generateNumberlessAccount(final UUID uuid) {
    return generateAccount(null, uuid, null);
  }

  private static Account generateAccount(@Nullable final String number, final UUID uuid, @Nullable final UUID pni) {
    Device device = generateDevice(DEVICE_ID_1);
    return generateAccount(number, uuid, pni, List.of(device));
  }

  private static Account generateAccount(@Nullable final String number, final UUID uuid, @Nullable final UUID pni, final List<Device> devices) {
    return generateAccount(number, uuid, pni, devices, TestRandomUtil.nextBytes(16));
  }

  private static Account generateAccount(@Nullable final String number, final UUID uuid, @Nullable final UUID pni, final List<Device> devices, final byte[] accountRecoveryPassword) {
    final byte[] unidentifiedAccessKey = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
    final Random random = new Random(System.currentTimeMillis());
    Arrays.fill(unidentifiedAccessKey, (byte) random.nextInt(255));

    return AccountsHelper.generateTestAccount(number, uuid, pni, devices, unidentifiedAccessKey, accountRecoveryPassword);
  }

  private void assertPhoneNumberConstraintExists(final String number, final UUID uuid) {
    final GetItemResponse numberConstraintResponse = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(Tables.NUMBERS.tableName())
            .key(Map.of(Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(number)))
            .build());

    assertThat(numberConstraintResponse.hasItem()).isTrue();
    assertThat(AttributeValues.getUUID(numberConstraintResponse.item(), Accounts.KEY_ACCOUNT_UUID, null)).isEqualTo(uuid);
  }

  private void assertPhoneNumberConstraintDoesNotExist(final String number) {
    final GetItemResponse numberConstraintResponse = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(Tables.NUMBERS.tableName())
            .key(Map.of(Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(number)))
            .build());

    assertThat(numberConstraintResponse.hasItem()).isFalse();
  }

  private void assertPhoneNumberIdentifierConstraintExists(final UUID phoneNumberIdentifier, final UUID uuid) {
    final GetItemResponse pniConstraintResponse = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(Tables.PNI_ASSIGNMENTS.tableName())
            .key(Map.of(Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier)))
            .build());

    assertThat(pniConstraintResponse.hasItem()).isTrue();
    assertThat(AttributeValues.getUUID(pniConstraintResponse.item(), Accounts.KEY_ACCOUNT_UUID, null)).isEqualTo(uuid);
  }

  private void assertPhoneNumberIdentifierConstraintDoesNotExist(final UUID phoneNumberIdentifier) {
    final GetItemResponse pniConstraintResponse = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(Tables.PNI_ASSIGNMENTS.tableName())
            .key(Map.of(Accounts.ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier)))
            .build());

    assertThat(pniConstraintResponse.hasItem()).isFalse();
  }

  private void assertRedeemedReceiptConstraintExists(final ReceiptCredentialPresentation receiptCredentialPresentation, final UUID accountId) {
    final GetItemResponse receiptConstraintResponse = DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(
        GetItemRequest.builder()
            .tableName(Tables.REDEEMED_RECEIPTS.tableName())
            .key(Map.of(RedeemedReceiptsManager.KEY_SERIAL, AttributeValues.b(receiptCredentialPresentation.getReceiptSerial().serialize())))
            .build()
    );
    assertThat(receiptConstraintResponse.hasItem()).isTrue();
    assertThat(AttributeValues.getUUID(receiptConstraintResponse.item(), RedeemedReceiptsManager.ATTR_ACCOUNT_UUID, null)).isEqualTo(accountId);
    assertThat(AttributeValues.getLong(receiptConstraintResponse.item(), RedeemedReceiptsManager.ATTR_RECEIPT_LEVEL, -1)).isEqualTo(receiptCredentialPresentation.getReceiptLevel());
    assertThat(AttributeValues.getLong(receiptConstraintResponse.item(), RedeemedReceiptsManager.ATTR_RECEIPT_EXPIRATION, -1)).isEqualTo(receiptCredentialPresentation.getReceiptExpirationTime());
  }

  private Map<String, AttributeValue> readAccount(final UUID uuid) {
    final DynamoDbClient db = DYNAMO_DB_EXTENSION.getDynamoDbClient();

    final GetItemResponse get = db.getItem(GetItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
        .consistentRead(true)
        .build());
    return get.item();
  }

  private Map<String, AttributeValue> getUsernameConstraintTableItem(final byte[] usernameHash) {
    return DYNAMO_DB_EXTENSION.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(Tables.USERNAMES.tableName())
            .key(Map.of(Accounts.UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash)))
            .build())
        .item();
  }

  @SuppressWarnings({"SameParameterValue", "OptionalUsedAsFieldOrParameterType"})
  private void verifyStoredState(Optional<String> maybeNumber, UUID uuid, Optional<UUID> maybePni, byte[] usernameHash, Account expecting, boolean canonicallyDiscoverable) {
    final DynamoDbClient db = DYNAMO_DB_EXTENSION.getDynamoDbClient();

    final GetItemResponse get = db.getItem(GetItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
        .consistentRead(true)
        .build());

    if (get.hasItem()) {
      String data = new String(get.item().get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), StandardCharsets.UTF_8);
      assertThat(data).isNotEmpty();

      assertThat(AttributeValues.getInt(get.item(), Accounts.ATTR_VERSION, -1))
          .isEqualTo(expecting.getVersion());

      assertThat(AttributeValues.getBool(get.item(), Accounts.ATTR_CANONICALLY_DISCOVERABLE,
          !canonicallyDiscoverable)).isEqualTo(canonicallyDiscoverable);

      assertThat(AttributeValues.getByteArray(get.item(), Accounts.ATTR_UAK, null))
          .isEqualTo(expecting.getUnidentifiedAccessKey().orElse(null));

      assertArrayEquals(AttributeValues.getByteArray(get.item(), Accounts.ATTR_USERNAME_HASH, null), usernameHash);

      Account result = Accounts.fromItem(get.item());
      verifyStoredState(maybeNumber, uuid, maybePni, usernameHash, result, expecting);
    } else {
      throw new AssertionError("No data");
    }
  }

  private void verifyAccountEquals(Account result, Account expecting) {
    verifyStoredState(
        expecting.getNumber(),
        expecting.getAccountIdentifier(),
        expecting.getPhoneNumberIdentifier(),
        expecting.getUsernameHash().orElse(null),
        result,
        expecting);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void verifyStoredState(Optional<String> maybeNumber, UUID uuid, Optional<UUID> maybePni, byte[] usernameHash, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(maybeNumber);
    assertThat(result.getPhoneNumberIdentifier()).isEqualTo(maybePni);
    assertThat(result.getLastSeen()).isEqualTo(expecting.getLastSeen());
    assertThat(result.getAccountIdentifier()).isEqualTo(uuid);
    assertThat(result.getVersion()).isEqualTo(expecting.getVersion());
    assertArrayEquals(result.getUsernameHash().orElse(null), usernameHash);
    assertArrayEquals(expecting.getUnidentifiedAccessKey().orElseThrow(), result.getUnidentifiedAccessKey().orElseThrow());

    for (final Device expectingDevice : expecting.getDevices()) {
      final Device resultDevice = result.getDevice(expectingDevice.getId()).orElseThrow();
      assertThat(resultDevice.getApnId()).isEqualTo(expectingDevice.getApnId());
      assertThat(resultDevice.getGcmId()).isEqualTo(expectingDevice.getGcmId());
      assertThat(resultDevice.getLastSeen()).isEqualTo(expectingDevice.getLastSeen());
      assertThat(resultDevice.getFetchesMessages()).isEqualTo(expectingDevice.getFetchesMessages());
      assertThat(resultDevice.getUserAgent()).isEqualTo(expectingDevice.getUserAgent());
      assertThat(resultDevice.getName()).isEqualTo(expectingDevice.getName());
      assertThat(resultDevice.getCreated()).isEqualTo(expectingDevice.getCreated());
      assertThat(resultDevice.getPhoneNumberIdentityRegistrationId()).isEqualTo(expectingDevice.getPhoneNumberIdentityRegistrationId());
    }
  }
}
