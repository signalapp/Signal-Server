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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
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
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
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

      // This is an unrelated table used to test "tag-along" transactional updates
      Tables.CLIENT_RELEASES);

  private final TestClock clock = TestClock.pinned(Instant.EPOCH);
  private DynamicConfigurationManager<DynamicConfiguration> mockDynamicConfigManager;
  private Accounts accounts;

  @BeforeEach
  void setupAccountsDao() {

    @SuppressWarnings("unchecked") DynamicConfigurationManager<DynamicConfiguration> m = mock(DynamicConfigurationManager.class);
    mockDynamicConfigManager = m;

    when(mockDynamicConfigManager.getConfiguration())
        .thenReturn(new DynamicConfiguration());

    accounts = new Accounts(
        clock,
        DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName());
  }

  @Test
  public void testStoreAndLookupUsernameLink() {
    final Account account = nextRandomAccount();
    account.setUsernameHash(TestRandomUtil.nextBytes(16));
    createAccount(account);

    final BiConsumer<Optional<Account>, byte[]> validator = (maybeAccount, expectedEncryptedUsername) -> {
      assertTrue(maybeAccount.isPresent());
      assertTrue(maybeAccount.get().getEncryptedUsername().isPresent());
      assertEquals(account.getUuid(), maybeAccount.get().getUuid());
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

  @Test
  void testStore() {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    boolean freshUser = createAccount(account);

    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    freshUser = createAccount(account);
    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());
  }

  @Test
  void testStoreRecentlyDeleted() {
    final UUID originalUuid = UUID.randomUUID();

    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", originalUuid, UUID.randomUUID(), List.of(device));

    boolean freshUser = createAccount(account);

    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    accounts.delete(originalUuid, Collections.emptyList()).join();
    assertThat(accounts.findRecentlyDeletedAccountIdentifier(account.getNumber())).hasValue(originalUuid);

    freshUser = createAccount(account);
    assertThat(freshUser).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    assertThat(accounts.findRecentlyDeletedAccountIdentifier(account.getNumber())).isEmpty();
  }

  @Test
  void testStoreMulti() {
    final List<Device> devices = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));
    final Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), devices);

    createAccount(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());
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

    verifyStoredState("+14151112222", uuidFirst, pniFirst, null, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, null, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByAccountIdentifier(uuidFirst);
    retrievedSecond = accounts.getByAccountIdentifier(uuidSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, null, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, null, retrievedSecond.get(), accountSecond);

    retrievedFirst = accounts.getByPhoneNumberIdentifier(pniFirst);
    retrievedSecond = accounts.getByPhoneNumberIdentifier(pniSecond);

    assertThat(retrievedFirst.isPresent()).isTrue();
    assertThat(retrievedSecond.isPresent()).isTrue();

    verifyStoredState("+14151112222", uuidFirst, pniFirst, null, retrievedFirst.get(), accountFirst);
    verifyStoredState("+14152221111", uuidSecond, pniSecond, null, retrievedSecond.get(), accountSecond);
  }

  @Test
  void testRetrieveNoPni() throws JsonProcessingException {
    final List<Device> devices = List.of(generateDevice(DEVICE_ID_1), generateDevice(DEVICE_ID_2));
    final UUID uuid = UUID.randomUUID();
    final Account account = generateAccount("+14151112222", uuid, null, devices);

    // Accounts#create enforces that newly-created accounts have a PNI, so we need to make a bit of an end-run around it
    // to simulate an existing account with no PNI.
    {
      final TransactWriteItem phoneNumberConstraintPut = TransactWriteItem.builder()
          .put(
              Put.builder()
                  .tableName(Tables.NUMBERS.tableName())
                  .item(Map.of(
                      Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber()),
                      Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
                  .conditionExpression(
                      "attribute_not_exists(#number) OR (attribute_exists(#number) AND #uuid = :uuid)")
                  .expressionAttributeNames(
                      Map.of("#uuid", Accounts.KEY_ACCOUNT_UUID,
                          "#number", Accounts.ATTR_ACCOUNT_E164))
                  .expressionAttributeValues(
                      Map.of(":uuid", AttributeValues.fromUUID(account.getUuid())))
                  .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                  .build())
          .build();

      final TransactWriteItem accountPut = TransactWriteItem.builder()
          .put(Put.builder()
              .tableName(Tables.ACCOUNTS.tableName())
              .conditionExpression("attribute_not_exists(#number) OR #number = :number")
              .expressionAttributeNames(Map.of("#number", Accounts.ATTR_ACCOUNT_E164))
              .expressionAttributeValues(Map.of(":number", AttributeValues.fromString(account.getNumber())))
              .item(Map.of(
                  Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                  Accounts.ATTR_ACCOUNT_E164, AttributeValues.fromString(account.getNumber()),
                  Accounts.ATTR_ACCOUNT_DATA, AttributeValues.fromByteArray(SystemMapper.jsonMapper().writeValueAsBytes(account)),
                  Accounts.ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
                  Accounts.ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.shouldBeVisibleInDirectory())))
              .build())
          .build();

      DYNAMO_DB_EXTENSION.getDynamoDbClient().transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(phoneNumberConstraintPut, accountPut)
          .build());
    }

    Optional<Account> retrieved = accounts.getByE164("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", uuid, null, null, retrieved.get(), account);

    retrieved = accounts.getByAccountIdentifier(uuid);

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", uuid, null, null, retrieved.get(), account);
  }

  // State before the account is re-registered
  enum UsernameStatus {
    NONE,
    RESERVED,
    RESERVED_WITH_SAVED_LINK,
    CONFIRMED
  }

  @ParameterizedTest
  @EnumSource(UsernameStatus.class)
  void reclaimAccountWithNoUsername(UsernameStatus usernameStatus) {
    Device device = generateDevice(DEVICE_ID_1);
    UUID firstUuid = UUID.randomUUID();
    UUID firstPni = UUID.randomUUID();
    Account account = generateAccount("+14151112222", firstUuid, firstPni, List.of(device));
    createAccount(account);

    final byte[] usernameHash = randomBytes(32);
    final byte[] encryptedUsername = randomBytes(32);
    switch (usernameStatus) {
      case NONE:
        break;
      case RESERVED:
        accounts.reserveUsernameHash(account, randomBytes(32), Duration.ofMinutes(1)).join();
        break;
      case RESERVED_WITH_SAVED_LINK:
        // give the account a username
        accounts.reserveUsernameHash(account, usernameHash, Duration.ofMinutes(1)).join();
        accounts.confirmUsernameHash(account, usernameHash, encryptedUsername).join();

        // simulate a partially-completed re-reg: we give the account a reclaimable username, but we'll try
        // re-registering again later in the test case
        account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));
        reclaimAccount(account);
        break;
      case CONFIRMED:
        accounts.reserveUsernameHash(account, usernameHash, Duration.ofMinutes(1)).join();
        accounts.confirmUsernameHash(account, usernameHash, encryptedUsername).join();
        break;
    }

    Optional<UUID> preservedLink = Optional.ofNullable(account.getUsernameLinkHandle());

    // re-register the account
    account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));
    reclaimAccount(account);

    // If we had a username link, or we had previously saved a username link from another re-registration, make sure
    // we preserve it
    accounts.confirmUsernameHash(account, usernameHash, encryptedUsername).join();

    boolean shouldReuseLink = switch (usernameStatus) {
      case RESERVED_WITH_SAVED_LINK, CONFIRMED -> true;
      case NONE, RESERVED -> false;
    };

    // If we had a reclaimable username, make sure we preserved the link.
    assertThat(account.getUsernameLinkHandle().equals(preservedLink.orElse(null)))
        .isEqualTo(shouldReuseLink);

    // in all cases, we should now have usernameHash, usernameLink, and encryptedUsername set
    assertThat(account.getUsernameHash()).isNotEmpty();
    assertThat(account.getEncryptedUsername()).isNotEmpty();
    assertThat(account.getUsernameLinkHandle()).isNotNull();
    assertThat(account.getReservedUsernameHash()).isEmpty();
  }

  private void reclaimAccount(final Account reregisteredAccount) {
    final AccountAlreadyExistsException accountAlreadyExistsException =
        assertThrows(AccountAlreadyExistsException.class,
            () -> accounts.create(reregisteredAccount, Collections.emptyList()));

    reregisteredAccount.setUuid(accountAlreadyExistsException.getExistingAccount().getUuid());
    reregisteredAccount.setNumber(accountAlreadyExistsException.getExistingAccount().getNumber(),
        accountAlreadyExistsException.getExistingAccount().getPhoneNumberIdentifier());

    assertDoesNotThrow(() -> accounts.reclaimAccount(accountAlreadyExistsException.getExistingAccount(),
        reregisteredAccount,
        Collections.emptyList()).toCompletableFuture().join());
  }

  @Test
  void testReclaimAccount() {
    final String e164 = "+14151112222";
    final Device device = generateDevice(DEVICE_ID_1);
    final UUID existingUuid = UUID.randomUUID();
    final UUID existingPni = UUID.randomUUID();
    final Account existingAccount = generateAccount(e164, existingUuid, existingPni, List.of(device));

    createAccount(existingAccount);

    final byte[] usernameHash = randomBytes(32);
    final byte[] encryptedUsername = randomBytes(16);

    // Set up the existing account to have a username hash
    accounts.confirmUsernameHash(existingAccount, usernameHash, encryptedUsername).join();
    final UUID usernameLinkHandle = existingAccount.getUsernameLinkHandle();

    verifyStoredState(e164, existingAccount.getUuid(), existingAccount.getPhoneNumberIdentifier(), usernameHash, existingAccount, true);

    assertPhoneNumberConstraintExists(e164, existingUuid);
    assertPhoneNumberIdentifierConstraintExists(existingPni, existingUuid);

    assertDoesNotThrow(() -> accounts.update(existingAccount));

    final UUID secondUuid = UUID.randomUUID();

    final Device secondDevice = generateDevice(DEVICE_ID_1);
    final Account secondAccount = generateAccount(e164, secondUuid, UUID.randomUUID(), List.of(secondDevice));

    reclaimAccount(secondAccount);

    // usernameHash should be unset
    verifyStoredState("+14151112222", existingUuid, existingPni, null, secondAccount, true);

    // username should become 'reclaimable'
    Map<String, AttributeValue> item = readAccount(existingUuid);
    Account result = Accounts.fromItem(item);
    assertThat(AttributeValues.getUUID(item, Accounts.ATTR_USERNAME_LINK_UUID, null))
        .isEqualTo(usernameLinkHandle)
        .isEqualTo(result.getUsernameLinkHandle());
    assertThat(result.getUsernameHash()).isEmpty();
    assertThat(result.getEncryptedUsername()).isEmpty();
    assertArrayEquals(result.getReservedUsernameHash().get(), usernameHash);

    // should keep the same usernameLink, now encryptedUsername should be set
    accounts.confirmUsernameHash(result, usernameHash, encryptedUsername).join();
    item = readAccount(existingUuid);
    result = Accounts.fromItem(item);
    assertThat(AttributeValues.getUUID(item, Accounts.ATTR_USERNAME_LINK_UUID, null))
        .isEqualTo(usernameLinkHandle)
        .isEqualTo(result.getUsernameLinkHandle());
    assertArrayEquals(result.getEncryptedUsername().get(), encryptedUsername);
    assertArrayEquals(result.getUsernameHash().get(), usernameHash);
    assertThat(result.getReservedUsernameHash()).isEmpty();

    assertPhoneNumberConstraintExists("+14151112222", existingUuid);
    assertPhoneNumberIdentifierConstraintExists(existingPni, existingUuid);

    Account invalidAccount = generateAccount("+14151113333", existingUuid, UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

    assertThatThrownBy(() -> createAccount(invalidAccount));
  }

  @Test
  void testUpdate() {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    createAccount(account);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    device.setName("foobar".getBytes(StandardCharsets.UTF_8));

    accounts.update(account);

    assertPhoneNumberConstraintExists("+14151112222", account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(account.getPhoneNumberIdentifier(), account.getUuid());

    Optional<Account> retrieved = accounts.getByE164("+14151112222");

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);

    retrieved = accounts.getByAccountIdentifier(account.getUuid());

    assertThat(retrieved.isPresent()).isTrue();
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    device = generateDevice(DEVICE_ID_1);
    Account unknownAccount = generateAccount("+14151113333", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    assertThatThrownBy(() -> accounts.update(unknownAccount)).isInstanceOfAny(ConditionalCheckFailedException.class);

    accounts.update(account);

    assertThat(account.getVersion()).isEqualTo(2);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);

    account.setVersion(1);

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);

    account.setVersion(2);

    accounts.update(account);

    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdateWithMockTransactionConflictException(boolean wrapException) {

    final DynamoDbAsyncClient dynamoDbAsyncClient = mock(DynamoDbAsyncClient.class);
    accounts = new Accounts(mock(DynamoDbClient.class),
        dynamoDbAsyncClient,
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName());

    Exception e = TransactionConflictException.builder().build();
    e = wrapException ? new CompletionException(e) : e;

    when(dynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(e));

    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID());

    assertThatThrownBy(() -> accounts.update(account)).isInstanceOfAny(ContestedOptimisticLockException.class);
  }

  @Test
  void testUpdateTransactionally() {
    final Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    final byte[] deviceName = "device-name".getBytes(StandardCharsets.UTF_8);

    assertNotEquals(deviceName,
        accounts.getByAccountIdentifier(account.getUuid()).orElseThrow().getPrimaryDevice().getName());

    assertFalse(DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(Tables.CLIENT_RELEASES.tableName())
            .key(Map.of(
                ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
            ))
            .build())
        .hasItem());

    account.getPrimaryDevice().setName(deviceName);

    accounts.updateTransactionallyAsync(account, List.of(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(Tables.CLIENT_RELEASES.tableName())
            .item(Map.of(
                ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
            ))
            .build())
        .build())).toCompletableFuture().join();

    assertArrayEquals(deviceName,
        accounts.getByAccountIdentifier(account.getUuid()).orElseThrow().getPrimaryDevice().getName());

    assertTrue(DYNAMO_DB_EXTENSION.getDynamoDbClient().getItem(GetItemRequest.builder()
            .tableName(Tables.CLIENT_RELEASES.tableName())
            .key(Map.of(
                ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
            ))
            .build())
        .hasItem());
  }

  @Test
  void testUpdateTransactionallyContestedLock() {
    final Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    account.setVersion(account.getVersion() - 1);

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> accounts.updateTransactionallyAsync(account, List.of(TransactWriteItem.builder()
            .put(Put.builder()
                .tableName(Tables.CLIENT_RELEASES.tableName())
                .item(Map.of(
                    ClientReleases.ATTR_PLATFORM, AttributeValues.fromString("test"),
                    ClientReleases.ATTR_VERSION, AttributeValues.fromString("test")
                ))
                .build())
            .build())).toCompletableFuture().join());

    assertTrue(completionException.getCause() instanceof ContestedOptimisticLockException);
  }

  @Test
  void testUpdateTransactionallyWithMockTransactionConflictException() {
    final DynamoDbAsyncClient dynamoDbAsyncClient = mock(DynamoDbAsyncClient.class);

    accounts = new Accounts(mock(DynamoDbClient.class),
        dynamoDbAsyncClient,
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName());

    when(dynamoDbAsyncClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(TransactionCanceledException.builder()
            .cancellationReasons(CancellationReason.builder()
                .code("TransactionConflict")
                .build())
            .build()));

    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID());

    assertThatThrownBy(() -> accounts.updateTransactionallyAsync(account, Collections.emptyList()).toCompletableFuture().join())
        .isInstanceOfAny(CompletionException.class)
        .hasCauseInstanceOf(ContestedOptimisticLockException.class);
  }

  @Test
  void testGetAll() {
    final List<Account> expectedAccounts = new ArrayList<>();

    for (int i = 1; i <= 100; i++) {
      final Account account = generateAccount("+1" + String.format("%03d", i), UUID.randomUUID(), UUID.randomUUID());
      expectedAccounts.add(account);
      createAccount(account);
    }

    final List<Account> retrievedAccounts =
        accounts.getAll(2, Schedulers.parallel()).collectList().block();

    assertNotNull(retrievedAccounts);
    assertEquals(expectedAccounts.stream().map(Account::getUuid).collect(Collectors.toSet()),
        retrievedAccounts.stream().map(Account::getUuid).collect(Collectors.toSet()));
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

    assertThat(accounts.findRecentlyDeletedAccountIdentifier(deletedAccount.getNumber())).isEmpty();

    assertPhoneNumberConstraintExists("+14151112222", deletedAccount.getUuid());
    assertPhoneNumberIdentifierConstraintExists(deletedAccount.getPhoneNumberIdentifier(), deletedAccount.getUuid());
    assertPhoneNumberConstraintExists("+14151112345", retainedAccount.getUuid());
    assertPhoneNumberIdentifierConstraintExists(retainedAccount.getPhoneNumberIdentifier(), retainedAccount.getUuid());

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getUuid())).isPresent();
    assertThat(accounts.getByAccountIdentifier(retainedAccount.getUuid())).isPresent();

    accounts.delete(deletedAccount.getUuid(), Collections.emptyList()).join();

    assertThat(accounts.getByAccountIdentifier(deletedAccount.getUuid())).isNotPresent();
    assertThat(accounts.findRecentlyDeletedAccountIdentifier(deletedAccount.getNumber())).hasValue(deletedAccount.getUuid());

    assertPhoneNumberConstraintDoesNotExist(deletedAccount.getNumber());
    assertPhoneNumberIdentifierConstraintDoesNotExist(deletedAccount.getPhoneNumberIdentifier());

    verifyStoredState(retainedAccount.getNumber(), retainedAccount.getUuid(), retainedAccount.getPhoneNumberIdentifier(),
        null, accounts.getByAccountIdentifier(retainedAccount.getUuid()).get(), retainedAccount);

    {
      final Account recreatedAccount = generateAccount(deletedAccount.getNumber(), UUID.randomUUID(),
          UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

      final boolean freshUser = createAccount(recreatedAccount);

      assertThat(freshUser).isTrue();
      assertThat(accounts.getByAccountIdentifier(recreatedAccount.getUuid())).isPresent();
      verifyStoredState(recreatedAccount.getNumber(), recreatedAccount.getUuid(), recreatedAccount.getPhoneNumberIdentifier(),
          null, accounts.getByAccountIdentifier(recreatedAccount.getUuid()).get(), recreatedAccount);

      assertPhoneNumberConstraintExists(recreatedAccount.getNumber(), recreatedAccount.getUuid());
      assertPhoneNumberIdentifierConstraintExists(recreatedAccount.getPhoneNumberIdentifier(), recreatedAccount.getUuid());
    }
  }

  @Test
  void testMissing() {
    Device device = generateDevice(DEVICE_ID_1);
    Account account = generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(device));

    createAccount(account);

    Optional<Account> retrieved = accounts.getByE164("+11111111");
    assertThat(retrieved.isPresent()).isFalse();

    retrieved = accounts.getByAccountIdentifier(UUID.randomUUID());
    assertThat(retrieved.isPresent()).isFalse();
  }

  @Test
  void getByAccountIdentifierAsync() {
    assertThat(accounts.getByAccountIdentifierAsync(UUID.randomUUID()).join()).isEmpty();

    final Account account =
        generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

    createAccount(account);

    assertThat(accounts.getByAccountIdentifierAsync(account.getUuid()).join()).isPresent();
  }

  @Test
  void getByPhoneNumberIdentifierAsync() {
    assertThat(accounts.getByPhoneNumberIdentifierAsync(UUID.randomUUID()).join()).isEmpty();

    final Account account =
        generateAccount("+14151112222", UUID.randomUUID(), UUID.randomUUID(), List.of(generateDevice(DEVICE_ID_1)));

    createAccount(account);

    assertThat(accounts.getByPhoneNumberIdentifierAsync(account.getPhoneNumberIdentifier()).join()).isPresent();
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
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, false);
    account.setDiscoverableByPhoneNumber(true);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, true);
    account.setDiscoverableByPhoneNumber(false);
    accounts.update(account);
    verifyStoredState("+14151112222", account.getUuid(), account.getPhoneNumberIdentifier(), null, account, false);
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

    assertPhoneNumberConstraintExists(originalNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getUuid());

    {
      final Optional<Account> retrieved = accounts.getByE164(originalNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(originalNumber, account.getUuid(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);
    }

    accounts.changeNumber(account, targetNumber, targetPni, maybeDisplacedAccountIdentifier, Collections.emptyList());

    assertThat(accounts.getByE164(originalNumber)).isEmpty();
    assertThat(accounts.getByAccountIdentifier(originalPni)).isEmpty();

    assertPhoneNumberConstraintDoesNotExist(originalNumber);
    assertPhoneNumberIdentifierConstraintDoesNotExist(originalPni);
    assertPhoneNumberConstraintExists(targetNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(targetPni, account.getUuid());

    {
      final Optional<Account> retrieved = accounts.getByE164(targetNumber);
      assertThat(retrieved).isPresent();

      verifyStoredState(targetNumber, account.getUuid(), account.getPhoneNumberIdentifier(), null, retrieved.get(), account);

      assertThat(retrieved.get().getPhoneNumberIdentifier()).isEqualTo(targetPni);
      assertThat(accounts.getByPhoneNumberIdentifier(targetPni)).isPresent();
    }

    assertThat(accounts.findRecentlyDeletedAccountIdentifier(originalNumber)).isEqualTo(maybeDisplacedAccountIdentifier);
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

    assertThrows(TransactionCanceledException.class, () -> accounts.changeNumber(account, targetNumber, targetPni, Optional.of(existingAccount.getUuid()), Collections.emptyList()));

    assertPhoneNumberConstraintExists(originalNumber, account.getUuid());
    assertPhoneNumberIdentifierConstraintExists(originalPni, account.getUuid());
    assertPhoneNumberConstraintExists(targetNumber, existingAccount.getUuid());
    assertPhoneNumberIdentifierConstraintExists(targetPni, existingAccount.getUuid());
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

    final Account secondAccountInstance = accounts.getByAccountIdentifier(firstAccountInstance.getUuid()).orElseThrow();

    // update via the first instance, which will update the version
    firstAccountInstance.setCurrentProfileVersion("1");
    accounts.update(firstAccountInstance);

    assertThrows(ContestedOptimisticLockException.class,
        () -> accounts.changeNumber(secondAccountInstance, targetNumber, targetPni, Optional.empty(),
            Collections.emptyList()), "Second account instance has stale version");

    final Account refreshedAccountInstance = accounts.getByAccountIdentifier(firstAccountInstance.getUuid())
        .orElseThrow();
    accounts.changeNumber(refreshedAccountInstance, targetNumber, targetPni, Optional.empty(),
        Collections.emptyList());

    assertPhoneNumberConstraintDoesNotExist(originalNumber);
    assertPhoneNumberIdentifierConstraintDoesNotExist(originalPni);
    assertPhoneNumberConstraintExists(targetNumber, firstAccountInstance.getUuid());
    assertPhoneNumberIdentifierConstraintExists(targetPni, firstAccountInstance.getUuid());
  }

  @Test
  void testSwitchUsernameHashes() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    final UUID oldHandle = account.getUsernameLinkHandle();

    {
      final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_1).join();
      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(), USERNAME_HASH_1, maybeAccount.orElseThrow(), account);

      final Optional<Account> maybeAccount2 = accounts.getByUsernameLinkHandle(oldHandle).join();
      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(), USERNAME_HASH_1, maybeAccount2.orElseThrow(), account);
    }

    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofDays(1)).join();
    accounts.confirmUsernameHash(account, USERNAME_HASH_2, ENCRYPTED_USERNAME_2).join();
    final UUID newHandle = account.getUsernameLinkHandle();

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).isEmpty();
    assertThat(accounts.getByUsernameLinkHandle(oldHandle).join()).isEmpty();

    {
      final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_2).join();

      assertThat(maybeAccount).isPresent();
      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(),
          USERNAME_HASH_2, maybeAccount.get(), account);
      final Optional<Account> maybeAccount2 = accounts.getByUsernameLinkHandle(newHandle).join();
      verifyStoredState(account.getNumber(), account.getUuid(), account.getPhoneNumberIdentifier(),
          USERNAME_HASH_2, maybeAccount2.get(), account);
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
      accounts.reserveUsernameHash(firstAccount, USERNAME_HASH_1, Duration.ofDays(1)).join();
      accounts.confirmUsernameHash(firstAccount, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    });

    final Optional<Account> maybeAccount = accounts.getByUsernameHash(USERNAME_HASH_1).join();

    assertThat(maybeAccount).isPresent();
    verifyStoredState(firstAccount.getNumber(), firstAccount.getUuid(), firstAccount.getPhoneNumberIdentifier(), USERNAME_HASH_1, maybeAccount.get(), firstAccount);

    // throw an error if second account tries to reserve or confirm the same username hash
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.reserveUsernameHash(secondAccount, USERNAME_HASH_1, Duration.ofDays(1)));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.confirmUsernameHash(secondAccount, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));

    // throw an error if first account tries to reserve or confirm the username hash that it has already confirmed
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.reserveUsernameHash(firstAccount, USERNAME_HASH_1, Duration.ofDays(1)));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.confirmUsernameHash(firstAccount, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));

    assertThat(secondAccount.getReservedUsernameHash()).isEmpty();
    assertThat(secondAccount.getUsernameHash()).isEmpty();
  }

  @ParameterizedTest
  @MethodSource
  void testReserveUsernameHashTransactionConflict(final Optional<String> constraintCancellationString,
      final Optional<String> accountsCancellationString,
      final Class<Exception> expectedException) {
    final DynamoDbAsyncClient dbAsyncClient = mock(DynamoDbAsyncClient.class);

    accounts = new Accounts(mock(DynamoDbClient.class),
        dbAsyncClient,
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName());
    final Account account = generateAccount("+14155551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    final CancellationReason constraintCancellationReason = constraintCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    final CancellationReason accountsCancellationReason = accountsCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    when(dbAsyncClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(TransactionCanceledException.builder()
            .cancellationReasons(constraintCancellationReason, accountsCancellationReason)
            .build()));

    CompletableFutureTestUtil.assertFailsWithCause(expectedException,
        accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
  }

  private static Stream<Arguments> testReserveUsernameHashTransactionConflict() {
    return Stream.of(
        Arguments.of(Optional.of("TransactionConflict"), Optional.empty(), ContestedOptimisticLockException.class),
        Arguments.of(Optional.empty(), Optional.of("TransactionConflict"), ContestedOptimisticLockException.class),
        Arguments.of(Optional.of("ConditionalCheckFailed"), Optional.of("TransactionConflict"), UsernameHashNotAvailableException.class)
    );
  }

  @ParameterizedTest
  @MethodSource
  void testConfirmUsernameHashTransactionConflict(final Optional<String> constraintCancellationString,
      final Optional<String> accountsCancellationString,
      final Class<Exception> expectedException) {
    final DynamoDbAsyncClient dbAsyncClient = mock(DynamoDbAsyncClient.class);

    accounts = new Accounts(mock(DynamoDbClient.class),
        dbAsyncClient,
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName());
    final Account account = generateAccount("+14155551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    final CancellationReason constraintCancellationReason = constraintCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    final CancellationReason accountsCancellationReason = accountsCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    when(dbAsyncClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(TransactionCanceledException.builder()
            .cancellationReasons(constraintCancellationReason,
                accountsCancellationReason,
                CancellationReason.builder().build())
            .build()));

    CompletableFutureTestUtil.assertFailsWithCause(expectedException,
        accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
  }

  private static Stream<Arguments> testConfirmUsernameHashTransactionConflict() {
    return Stream.of(
        Arguments.of(Optional.of("TransactionConflict"), Optional.empty(), ContestedOptimisticLockException.class),
        Arguments.of(Optional.empty(), Optional.of("TransactionConflict"), ContestedOptimisticLockException.class),
        Arguments.of(Optional.of("ConditionalCheckFailed"), Optional.of("TransactionConflict"), UsernameHashNotAvailableException.class)
    );
  }

  @Test
  void testConfirmUsernameHashVersionMismatch() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    account.setVersion(account.getVersion() + 77);

    CompletableFutureTestUtil.assertFailsWithCause(ContestedOptimisticLockException.class,
        accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));

    assertThat(account.getUsernameHash()).isEmpty();
  }

  @Test
  void testClearUsername() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isPresent();

    accounts.clearUsernameHash(account).join();

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();
    assertThat(accounts.getByAccountIdentifier(account.getUuid()))
        .hasValueSatisfying(clearedAccount -> {
          assertThat(clearedAccount.getUsernameHash()).isEmpty();
          assertThat(clearedAccount.getUsernameLinkHandle()).isNull();
          assertThat(clearedAccount.getEncryptedUsername().isEmpty());
        });
  }

  @Test
  void testClearUsernameNoUsername() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    assertThatNoException().isThrownBy(() -> accounts.clearUsernameHash(account).join());
  }

  @Test
  void testClearUsernameVersionMismatch() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();

    account.setVersion(account.getVersion() + 12);

    CompletableFutureTestUtil.assertFailsWithCause(ContestedOptimisticLockException.class,
        accounts.clearUsernameHash(account));

    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
  }

  @ParameterizedTest
  @MethodSource
  void testClearUsernameTransactionConflict(final Optional<String> constraintCancellationString,
      final Optional<String> accountsCancellationString) {
    final DynamoDbAsyncClient dbAsyncClient = mock(DynamoDbAsyncClient.class);

    accounts = new Accounts(mock(DynamoDbClient.class),
        dbAsyncClient,
        Tables.ACCOUNTS.tableName(),
        Tables.NUMBERS.tableName(),
        Tables.PNI_ASSIGNMENTS.tableName(),
        Tables.USERNAMES.tableName(),
        Tables.DELETED_ACCOUNTS.tableName());

    final Account account = generateAccount("+14155551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    when(dbAsyncClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(mock(TransactWriteItemsResponse.class)));

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();

    final CancellationReason constraintCancellationReason = constraintCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    final CancellationReason accountsCancellationReason = accountsCancellationString.map(
        reason -> CancellationReason.builder().code(reason).build()
    ).orElse(CancellationReason.builder().build());

    when(dbAsyncClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(TransactionCanceledException.builder()
            .cancellationReasons(accountsCancellationReason, constraintCancellationReason)
            .build()));

    CompletableFutureTestUtil.assertFailsWithCause(ContestedOptimisticLockException.class,
        accounts.clearUsernameHash(account));

    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
  }

  private static Stream<Arguments> testClearUsernameTransactionConflict() {
    return Stream.of(
        Arguments.of(Optional.empty(), Optional.of("TransactionConflict"), ContestedOptimisticLockException.class),
        Arguments.of(Optional.of("TransactionConflict"), Optional.empty(), ContestedOptimisticLockException.class)
    );
  }

  @Test
  void testReservedUsernameHash() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1)).join();
    assertArrayEquals(account1.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account1.getUsernameHash()).isEmpty();

    // account 2 shouldn't be able to reserve or confirm the same username hash
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    accounts.confirmUsernameHash(account1, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    assertThat(account1.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(account1.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join().get().getUuid()).isEqualTo(account1.getUuid());

    final Map<String, AttributeValue> usernameConstraintRecord = getUsernameConstraintTableItem(USERNAME_HASH_1);

    assertThat(usernameConstraintRecord).containsKey(Accounts.ATTR_USERNAME_HASH);
    assertThat(usernameConstraintRecord).doesNotContainKey(Accounts.ATTR_TTL);
  }

  @Test
  void switchBetweenReservedUsernameHashes() {
    final Account account = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    assertArrayEquals(account.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account.getUsernameHash()).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_2, Duration.ofDays(1)).join();
    assertArrayEquals(account.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_2);
    assertThat(account.getUsernameHash()).isEmpty();

    final Map<String, AttributeValue> usernameConstraintRecord1 = getUsernameConstraintTableItem(USERNAME_HASH_1);
    final Map<String, AttributeValue> usernameConstraintRecord2 = getUsernameConstraintTableItem(USERNAME_HASH_2);
    assertThat(usernameConstraintRecord1).containsKey(Accounts.ATTR_USERNAME_HASH);
    assertThat(usernameConstraintRecord2).containsKey(Accounts.ATTR_USERNAME_HASH);
    assertThat(usernameConstraintRecord1).containsKey(Accounts.ATTR_TTL);
    assertThat(usernameConstraintRecord2).containsKey(Accounts.ATTR_TTL);

    clock.pin(Instant.EPOCH.plus(Duration.ofMinutes(1)));

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    assertArrayEquals(account.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account.getUsernameHash()).isEmpty();

    final Map<String, AttributeValue> newUsernameConstraintRecord1 = getUsernameConstraintTableItem(USERNAME_HASH_1);
    assertThat(newUsernameConstraintRecord1).containsKey(Accounts.ATTR_USERNAME_HASH);
    assertThat(newUsernameConstraintRecord1).containsKey(Accounts.ATTR_TTL);
    assertThat(usernameConstraintRecord1.get(Accounts.ATTR_TTL))
        .isNotEqualTo(newUsernameConstraintRecord1.get(Accounts.ATTR_TTL));
  }

  @Test
  void reserveOwnConfirmedUsername() {
    final Account account = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    assertArrayEquals(account.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account.getUsernameHash()).isEmpty();
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).containsKey(Accounts.ATTR_TTL);


    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).doesNotContainKey(Accounts.ATTR_TTL);

    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertArrayEquals(account.getUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).containsKey(Accounts.ATTR_USERNAME_HASH);
    assertThat(getUsernameConstraintTableItem(USERNAME_HASH_1)).doesNotContainKey(Accounts.ATTR_TTL);
  }

  @Test
  void testConfirmReservedUsernameHashWrongAccountUuid() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(1)).join();
    assertArrayEquals(account1.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);
    assertThat(account1.getUsernameHash()).isEmpty();

    // only account1 should be able to confirm the reserved hash
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
  }

  @Test
  void testConfirmExpiredReservedUsernameHash() {
    final Account account1 = generateAccount("+18005551111", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account1);
    final Account account2 = generateAccount("+18005552222", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account2);

    accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(2)).join();

    for (int i = 0; i <= 2; i++) {
      clock.pin(Instant.EPOCH.plus(Duration.ofDays(i)));
      CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
          accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)));
    }

    // after 2 days, can reserve and confirm the hash
    clock.pin(Instant.EPOCH.plus(Duration.ofDays(2)).plus(Duration.ofSeconds(1)));
    accounts.reserveUsernameHash(account2, USERNAME_HASH_1, Duration.ofDays(1)).join();
    assertEquals(account2.getReservedUsernameHash().orElseThrow(), USERNAME_HASH_1);

    accounts.confirmUsernameHash(account2, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();

    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.reserveUsernameHash(account1, USERNAME_HASH_1, Duration.ofDays(2)));
    CompletableFutureTestUtil.assertFailsWithCause(UsernameHashNotAvailableException.class,
        accounts.confirmUsernameHash(account1, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join().get().getUuid()).isEqualTo(account2.getUuid());
  }

  @Test
  void testReserveConfirmUsernameHashVersionConflict() {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);
    account.setVersion(account.getVersion() + 12);
    CompletableFutureTestUtil.assertFailsWithCause(ContestedOptimisticLockException.class,
        accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)));
    CompletableFutureTestUtil.assertFailsWithCause(ContestedOptimisticLockException.class,
        accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1));
    assertThat(account.getReservedUsernameHash()).isEmpty();
    assertThat(account.getUsernameHash()).isEmpty();
  }

  @Test
  public void testIgnoredFieldsNotAddedToDataAttribute() throws Exception {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    account.setUsernameHash(TestRandomUtil.nextBytes(32));
    account.setUsernameLinkDetails(UUID.randomUUID(), TestRandomUtil.nextBytes(32));
    createAccount(account);
    final Map<String, AttributeValue> accountRecord = DYNAMO_DB_EXTENSION.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(Tables.ACCOUNTS.tableName())
            .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
            .build())
        .item();
    final Map<?, ?> dataMap = SystemMapper.jsonMapper()
        .readValue(accountRecord.get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), Map.class);
    Accounts.ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION
        .forEach(field -> assertFalse(dataMap.containsKey(field)));
  }

  @Test
  void testGetByUsernameHashAsync() {
    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    createAccount(account);

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isEmpty();

    accounts.reserveUsernameHash(account, USERNAME_HASH_1, Duration.ofDays(1)).join();
    accounts.confirmUsernameHash(account, USERNAME_HASH_1, ENCRYPTED_USERNAME_1).join();

    assertThat(accounts.getByUsernameHash(USERNAME_HASH_1).join()).isPresent();
  }

  @Test
  public void testInvalidDeviceIdDeserialization() throws Exception {
    final Account account = generateAccount("+18005551234", UUID.randomUUID(), UUID.randomUUID());
    final Device device2 = generateDevice((byte) 64);
    account.addDevice(device2);

    createAccount(account);

    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().getItem(GetItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
        .build()).join();

    final Map<?, ?> accountData = SystemMapper.jsonMapper()
        .readValue(response.item().get(Accounts.ATTR_ACCOUNT_DATA).b().asByteArray(), Map.class);

    final List<Map<Object, Object>> devices = (List<Map<Object, Object>>) accountData.get("devices");
    assertEquals(Integer.valueOf(device2.getId()), devices.get(1).get("id"));

    devices.get(1).put("id", Byte.MAX_VALUE + 5);

    DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient().updateItem(UpdateItemRequest.builder()
        .tableName(Tables.ACCOUNTS.tableName())
        .key(Map.of(Accounts.KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
        .updateExpression("SET #data = :data")
        .expressionAttributeNames(Map.of("#data", Accounts.ATTR_ACCOUNT_DATA))
        .expressionAttributeValues(
            Map.of(":data", AttributeValues.fromByteArray(SystemMapper.jsonMapper().writeValueAsBytes(accountData))))
        .build()).join();

    final CompletionException e = assertThrows(CompletionException.class,
        () -> accounts.getByAccountIdentifierAsync(account.getUuid()).join());

    Throwable cause = e.getCause();
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }

    assertInstanceOf(DeviceIdDeserializer.DeviceIdDeserializationException.class, cause);
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

  private static Account nextRandomAccount() {
    final String nextNumber = "+1800%07d".formatted(ACCOUNT_COUNTER.getAndIncrement());
    return generateAccount(nextNumber, UUID.randomUUID(), UUID.randomUUID());
  }

  private static Account generateAccount(String number, UUID uuid, final UUID pni) {
    Device device = generateDevice(DEVICE_ID_1);
    return generateAccount(number, uuid, pni, List.of(device));
  }

  private static Account generateAccount(String number, UUID uuid, final UUID pni, List<Device> devices) {
    final byte[] unidentifiedAccessKey = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
    final Random random = new Random(System.currentTimeMillis());
    Arrays.fill(unidentifiedAccessKey, (byte) random.nextInt(255));

    return AccountsHelper.generateTestAccount(number, uuid, pni, devices, unidentifiedAccessKey);
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
            .key(Map.of(Accounts.ATTR_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash)))
            .build())
        .item();
  }

  private void verifyStoredState(String number, UUID uuid, UUID pni, byte[] usernameHash, Account expecting, boolean canonicallyDiscoverable) {
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
      verifyStoredState(number, uuid, pni, usernameHash, result, expecting);
    } else {
      throw new AssertionError("No data");
    }
  }

  private void verifyStoredState(String number, UUID uuid, UUID pni, byte[] usernameHash, Account result, Account expecting) {
    assertThat(result.getNumber()).isEqualTo(number);
    assertThat(result.getPhoneNumberIdentifier()).isEqualTo(pni);
    assertThat(result.getLastSeen()).isEqualTo(expecting.getLastSeen());
    assertThat(result.getUuid()).isEqualTo(uuid);
    assertThat(result.getVersion()).isEqualTo(expecting.getVersion());
    assertArrayEquals(result.getUsernameHash().orElse(null), usernameHash);
    assertThat(Arrays.equals(result.getUnidentifiedAccessKey().get(), expecting.getUnidentifiedAccessKey().get())).isTrue();

    for (Device expectingDevice : expecting.getDevices()) {
      Device resultDevice = result.getDevice(expectingDevice.getId()).get();
      assertThat(resultDevice.getApnId()).isEqualTo(expectingDevice.getApnId());
      assertThat(resultDevice.getGcmId()).isEqualTo(expectingDevice.getGcmId());
      assertThat(resultDevice.getLastSeen()).isEqualTo(expectingDevice.getLastSeen());
      assertThat(resultDevice.getFetchesMessages()).isEqualTo(expectingDevice.getFetchesMessages());
      assertThat(resultDevice.getUserAgent()).isEqualTo(expectingDevice.getUserAgent());
      assertThat(resultDevice.getName()).isEqualTo(expectingDevice.getName());
      assertThat(resultDevice.getCreated()).isEqualTo(expectingDevice.getCreated());
    }
  }

  private static byte[] randomBytes(int count) {
    byte[] bytes = new byte[count];
    ThreadLocalRandom.current().nextBytes(bytes);
    return bytes;
  }
}
