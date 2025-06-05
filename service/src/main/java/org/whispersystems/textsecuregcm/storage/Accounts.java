/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.util.AsyncTimerUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.CompletableFutureUtils;

/**
 * "Accounts" DDB table's structure doesn't match 1:1 the {@link Account} class: most of the class fields are serialized
 * and stored in the {@link Accounts#ATTR_ACCOUNT_DATA} attribute, however there are certain fields that are stored only as DDB attributes
 * (e.g. if indexing or lookup by field is required), and there are also fields that stored in both places.
 * This class contains all the logic that decides whether or not a field of the {@link Account} class should be
 * added as an attribute, serialized as a part of {@link Accounts#ATTR_ACCOUNT_DATA}, or both. To skip serialization,
 * make sure attribute name is listed in {@link Accounts#ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION}. If serialization is skipped,
 * make sure the field is stored in a DDB attribute and then put back into the account object in {@link Accounts#fromItem(Map)}.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class Accounts {

  private static final Logger log = LoggerFactory.getLogger(Accounts.class);

  private static final Duration USERNAME_RECLAIM_TTL = Duration.ofDays(3);

  static final List<String> ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION = List.of("uuid", "usernameLinkHandle");

  @VisibleForTesting
  static final ObjectWriter ACCOUNT_DDB_JSON_WRITER = SystemMapper.jsonMapper()
      .writer(SystemMapper.excludingField(Account.class, ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION));

  private static final Timer CREATE_TIMER = Metrics.timer(name(Accounts.class, "create"));
  private static final Timer CHANGE_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "changeNumber"));
  private static final Timer SET_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "setUsername"));
  private static final Timer RESERVE_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "reserveUsername"));
  private static final Timer CLEAR_USERNAME_HASH_TIMER = Metrics.timer(name(Accounts.class, "clearUsernameHash"));
  private static final Timer UPDATE_TIMER = Metrics.timer(name(Accounts.class, "update"));
  private static final Timer UPDATE_TRANSACTIONALLY_TIMER = Metrics.timer(name(Accounts.class, "updateTransactionally"));
  private static final Timer RECLAIM_TIMER = Metrics.timer(name(Accounts.class, "reclaim"));
  private static final Timer GET_BY_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "getByNumber"));
  private static final Timer GET_BY_USERNAME_HASH_TIMER = Metrics.timer(name(Accounts.class, "getByUsernameHash"));
  private static final Timer GET_BY_USERNAME_LINK_HANDLE_TIMER = Metrics.timer(name(Accounts.class, "getByUsernameLinkHandle"));
  private static final Timer GET_BY_PNI_TIMER = Metrics.timer(name(Accounts.class, "getByPni"));
  private static final Timer GET_BY_UUID_TIMER = Metrics.timer(name(Accounts.class, "getByUuid"));
  private static final Timer DELETE_TIMER = Metrics.timer(name(Accounts.class, "delete"));
  private static final String USERNAME_HOLD_ADDED_COUNTER_NAME = name(Accounts.class, "usernameHoldAdded");

  private static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

  private static final String TRANSACTION_CONFLICT = "TransactionConflict";

  // uuid, primary key
  static final String KEY_ACCOUNT_UUID = "U";
  // uuid, attribute on account table, primary key for PNI table
  static final String ATTR_PNI_UUID = "PNI";
  // uuid of the current username link or null
  static final String ATTR_USERNAME_LINK_UUID = "UL";
  // phone number
  static final String ATTR_ACCOUNT_E164 = "P";
  // account, serialized to JSON
  static final String ATTR_ACCOUNT_DATA = "D";
  // internal version for optimistic locking
  static final String ATTR_VERSION = "V";
  // canonically discoverable
  static final String ATTR_CANONICALLY_DISCOVERABLE = "C";
  // username hash; byte[] or null
  static final String ATTR_USERNAME_HASH = "N";

  // bytes, primary key
  static final String KEY_LINK_DEVICE_TOKEN_HASH = "H";

  // integer, seconds
  static final String ATTR_LINK_DEVICE_TOKEN_TTL = "E";

  // unidentified access key; byte[] or null
  static final String ATTR_UAK = "UAK";

  // For historical reasons, deleted-accounts PNI is stored as a string-format UUID rather than a
  // compact byte array.
  static final String DELETED_ACCOUNTS_KEY_ACCOUNT_PNI = "P";

  static final String DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID = "U";
  static final String DELETED_ACCOUNTS_ATTR_EXPIRES = "E";
  static final String DELETED_ACCOUNTS_UUID_TO_PNI_INDEX_NAME = "u_to_p";

  static final String USERNAME_LINK_TO_UUID_INDEX = "ul_to_u";

  static final Duration DELETED_ACCOUNTS_TIME_TO_LIVE = Duration.ofDays(30);

  /**
   * Maximum number of temporary username holds an account can have on recently used usernames
   */
  @VisibleForTesting
  static final int MAX_USERNAME_HOLDS = 3;

  /**
   * How long an old username is held for an account after the account initially clears/switches the username
   */
  @VisibleForTesting
  static final Duration USERNAME_HOLD_DURATION = Duration.ofDays(7);

  private final Clock clock;

  private final DynamoDbClient dynamoDbClient;
  private final DynamoDbAsyncClient dynamoDbAsyncClient;

  private final String phoneNumberConstraintTableName;
  private final String phoneNumberIdentifierConstraintTableName;
  private final String usernamesConstraintTableName;
  private final String deletedAccountsTableName;
  private final String usedLinkDeviceTokenTableName;
  private final String accountsTableName;

  public Accounts(
      final Clock clock,
      final DynamoDbClient dynamoDbClient,
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final String deletedAccountsTableName,
      final String usedLinkDeviceTokenTableName) {

    this.clock = clock;
    this.dynamoDbClient = dynamoDbClient;
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.phoneNumberConstraintTableName = phoneNumberConstraintTableName;
    this.phoneNumberIdentifierConstraintTableName = phoneNumberIdentifierConstraintTableName;
    this.accountsTableName = accountsTableName;
    this.usernamesConstraintTableName = usernamesConstraintTableName;
    this.deletedAccountsTableName = deletedAccountsTableName;
    this.usedLinkDeviceTokenTableName = usedLinkDeviceTokenTableName;
  }

  static class UsernameTable {
    // usernameHash; bytes.
    static final String KEY_USERNAME_HASH = Accounts.ATTR_USERNAME_HASH;
    // uuid, bytes. The owner of the username or reservation
    static final String ATTR_ACCOUNT_UUID = Accounts.KEY_ACCOUNT_UUID;
    // confirmed; bool
    static final String ATTR_CONFIRMED = "F";
    // reclaimable; bool. Indicates that on confirmation the username link should be preserved
    static final String ATTR_RECLAIMABLE = "R";
    // time to live; number
    static final String ATTR_TTL = "TTL";
  }

  boolean create(final Account account, final List<TransactWriteItem> additionalWriteItems)
      throws AccountAlreadyExistsException {

    final Timer.Sample sample = Timer.start();

    try {
      final AttributeValue uuidAttr = AttributeValues.fromUUID(account.getUuid());
      final AttributeValue numberAttr = AttributeValues.fromString(account.getNumber());
      final AttributeValue pniUuidAttr = AttributeValues.fromUUID(account.getPhoneNumberIdentifier());

      final TransactWriteItem phoneNumberConstraintPut = buildConstraintTablePutIfAbsent(
          phoneNumberConstraintTableName, uuidAttr, ATTR_ACCOUNT_E164, numberAttr);

      final TransactWriteItem phoneNumberIdentifierConstraintPut = buildConstraintTablePutIfAbsent(
          phoneNumberIdentifierConstraintTableName, uuidAttr, ATTR_PNI_UUID, pniUuidAttr);

      final TransactWriteItem accountPut = buildAccountPut(account, uuidAttr, numberAttr, pniUuidAttr);

      // Clear any "recently deleted account" record for this number since, if it existed, we've used its old ACI for
      // the newly-created account.
      final TransactWriteItem deletedAccountDelete = buildRemoveDeletedAccount(account.getPhoneNumberIdentifier());

      final Collection<TransactWriteItem> writeItems = new ArrayList<>(
          List.of(phoneNumberConstraintPut, phoneNumberIdentifierConstraintPut, accountPut, deletedAccountDelete));

      writeItems.addAll(additionalWriteItems);

      final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
          .transactItems(writeItems)
          .build();

      try {
        dynamoDbClient.transactWriteItems(request);
      } catch (final TransactionCanceledException e) {

        final CancellationReason accountCancellationReason = e.cancellationReasons().get(2);

        if (conditionalCheckFailed(accountCancellationReason)) {
          throw new IllegalArgumentException("account identifier present with different phone number");
        }

        final CancellationReason phoneNumberConstraintCancellationReason = e.cancellationReasons().get(0);
        final CancellationReason phoneNumberIdentifierConstraintCancellationReason = e.cancellationReasons().get(1);

        if (conditionalCheckFailed(phoneNumberConstraintCancellationReason)
            || conditionalCheckFailed(phoneNumberIdentifierConstraintCancellationReason)) {

          // Both reasons should trip in tandem and either should give us the information we need. However, phone number
          // canonicalization can cause multiple e164s to have the same PNI, so we make sure we're choosing a condition
          // check that really failed.
          final CancellationReason reason = conditionalCheckFailed(phoneNumberConstraintCancellationReason)
              ? phoneNumberConstraintCancellationReason
              : phoneNumberIdentifierConstraintCancellationReason;

          final UUID existingAccountUuid =
              UUIDUtil.fromByteBuffer(reason.item().get(KEY_ACCOUNT_UUID).b().asByteBuffer());

          // This is unlikely, but it could be that the existing account was deleted in between the time the transaction
          // happened and when we tried to read the full existing account. If that happens, we can just consider this a
          // contested lock, and retrying is likely to succeed.
          final Account existingAccount = getByAccountIdentifier(existingAccountUuid)
              .orElseThrow(ContestedOptimisticLockException::new);

          throw new AccountAlreadyExistsException(existingAccount);
        }

        if (TRANSACTION_CONFLICT.equals(accountCancellationReason.code())) {
          // this should only happen if two clients manage to make concurrent create() calls
          throw new ContestedOptimisticLockException();
        }

        // this shouldn't happen
        throw new RuntimeException("could not create account: " + extractCancellationReasonCodes(e));
      }
    } finally {
      sample.stop(CREATE_TIMER);
    }

    return true;
  }

  /**
   * Copies over any account attributes that should be preserved when a new account reclaims an account identifier.
   *
   * @param existingAccount the existing account in the accounts table
   * @param accountToCreate a new account, with the same number and identifier as existingAccount
   */
  CompletionStage<Void> reclaimAccount(final Account existingAccount,
      final Account accountToCreate,
      final Collection<TransactWriteItem> additionalWriteItems) {

    if (!existingAccount.getUuid().equals(accountToCreate.getUuid()) ||
        !existingAccount.getPhoneNumberIdentifier().equals(accountToCreate.getPhoneNumberIdentifier())) {

      log.error("Reclaimed accounts must match. Old account {}:{}:{}, New account {}:{}:{}",
          existingAccount.getUuid(), redactPhoneNumber(existingAccount.getNumber()), existingAccount.getPhoneNumberIdentifier(),
          accountToCreate.getUuid(), redactPhoneNumber(accountToCreate.getNumber()), accountToCreate.getPhoneNumberIdentifier());
      throw new IllegalArgumentException("reclaimed accounts must match");
    }

    return AsyncTimerUtil.record(RECLAIM_TIMER, () -> {

      accountToCreate.setVersion(existingAccount.getVersion());

      // Carry over the old backup id commitment. If the new account claimer cannot does not have the secret used to
      // generate their backup-id, this credential is useless, however if they can produce the same credential they
      // won't be rate-limited for setting their backup-id.
      accountToCreate.setBackupCredentialRequests(
          existingAccount.getBackupCredentialRequest(BackupCredentialType.MESSAGES).orElse(null),
          existingAccount.getBackupCredentialRequest(BackupCredentialType.MEDIA).orElse(null));

      final List<TransactWriteItem> writeItems = new ArrayList<>();

      // If we're reclaiming an account that already has a username, we'd like to give the re-registering client
      // an opportunity to reclaim their original username and link. We do this by:
      //   1. marking the usernameHash as reserved for the aci
      //   2. saving the username link id, but not the encrypted username. The link will be broken until the client
      //      reclaims their username
      //
      // If we partially reclaim the account but fail (for example, we update the account but the client goes away
      // before creation is finished), we might be reclaiming the account we already reclaimed. In that case, we
      // should copy over the reserved username and link verbatim
      if (existingAccount.getReservedUsernameHash().isPresent() &&
          existingAccount.getUsernameLinkHandle() != null &&
          existingAccount.getUsernameHash().isEmpty() &&
          existingAccount.getEncryptedUsername().isEmpty()) {
        // reclaiming a partially reclaimed account
        accountToCreate.setReservedUsernameHash(existingAccount.getReservedUsernameHash().get());
        accountToCreate.setUsernameLinkHandle(existingAccount.getUsernameLinkHandle());
      } else if (existingAccount.getUsernameHash().isPresent()) {
        // reclaiming an account with a username
        final byte[] usernameHash = existingAccount.getUsernameHash().get();
        final long expirationTime = clock.instant().plus(USERNAME_RECLAIM_TTL).getEpochSecond();
        accountToCreate.setReservedUsernameHash(usernameHash);
        accountToCreate.setUsernameLinkHandle(existingAccount.getUsernameLinkHandle());

        writeItems.add(TransactWriteItem.builder()
            .put(Put.builder()
                .tableName(usernamesConstraintTableName)
                .item(Map.of(
                    UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
                    UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(accountToCreate.getUuid()),
                    UsernameTable.ATTR_TTL, AttributeValues.fromLong(expirationTime),
                    UsernameTable.ATTR_CONFIRMED, AttributeValues.fromBool(false),
                    UsernameTable.ATTR_RECLAIMABLE, AttributeValues.fromBool(true)))
                .conditionExpression("attribute_not_exists(#username_hash) OR (#ttl < :now) OR #uuid = :uuid")
                .expressionAttributeNames(Map.of(
                    "#username_hash", UsernameTable.KEY_USERNAME_HASH,
                    "#ttl", UsernameTable.ATTR_TTL,
                    "#uuid", UsernameTable.ATTR_ACCOUNT_UUID))
                .expressionAttributeValues(Map.of(
                    ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                    ":uuid", AttributeValues.fromUUID(accountToCreate.getUuid())))
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build())
            .build());
      }
      writeItems.add(UpdateAccountSpec.forAccount(accountsTableName, accountToCreate).transactItem());
      writeItems.addAll(additionalWriteItems);

      return dynamoDbAsyncClient.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build())
          .thenApply(response -> {
            accountToCreate.setVersion(accountToCreate.getVersion() + 1);
            return (Void) null;
          })
          .exceptionally(throwable -> {
            final Throwable unwrapped = ExceptionUtils.unwrap(throwable);
            if (unwrapped instanceof TransactionCanceledException te) {
              if (te.cancellationReasons().stream().anyMatch(Accounts::conditionalCheckFailed)) {
                throw new ContestedOptimisticLockException();
              }
            }
            // rethrow
            throw CompletableFutureUtils.errorAsCompletionException(throwable);
          });
    });
  }

  /**
   * Changes the phone number for the given account. The given account's number should be its current, pre-change
   * number. If this method succeeds, the account's number will be changed to the new number and its phone number
   * identifier will be changed to the given phone number identifier. If the update fails for any reason, the account's
   * number and PNI will be unchanged.
   * <p/>
   * This method expects that any accounts with conflicting numbers will have been removed by the time this method is
   * called. This method may fail with an unspecified {@link RuntimeException} if another account with the same number
   * exists in the data store.
   *
   * @param account the account for which to change the phone number
   * @param number the new phone number
   */
  public void changeNumber(final Account account,
      final String number,
      final UUID phoneNumberIdentifier,
      final Optional<UUID> maybeDisplacedAccountIdentifier,
      final Collection<TransactWriteItem> additionalWriteItems) {

    CHANGE_NUMBER_TIMER.record(() -> {
      final String originalNumber = account.getNumber();
      final UUID originalPni = account.getPhoneNumberIdentifier();

      boolean succeeded = false;

      account.setNumber(number, phoneNumberIdentifier);

      int accountUpdateIndex = -1;
      try {
        final List<TransactWriteItem> writeItems = new ArrayList<>();
        final AttributeValue uuidAttr = AttributeValues.fromUUID(account.getUuid());
        final AttributeValue numberAttr = AttributeValues.fromString(number);
        final AttributeValue pniAttr = AttributeValues.fromUUID(phoneNumberIdentifier);

        writeItems.add(buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, originalNumber));
        writeItems.add(buildConstraintTablePut(phoneNumberConstraintTableName, uuidAttr, ATTR_ACCOUNT_E164, numberAttr));
        writeItems.add(buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, originalPni));
        writeItems.add(buildConstraintTablePut(phoneNumberIdentifierConstraintTableName, uuidAttr, ATTR_PNI_UUID, pniAttr));
        writeItems.add(buildRemoveDeletedAccount(phoneNumberIdentifier));
        maybeDisplacedAccountIdentifier.ifPresent(displacedAccountIdentifier ->
            writeItems.add(buildPutDeletedAccount(displacedAccountIdentifier, originalPni)));

        // The `catch (TransactionCanceledException) block needs to check whether the cancellation reason is the account
        // update write item
        accountUpdateIndex = writeItems.size();
        writeItems.add(
            TransactWriteItem.builder()
                .update(Update.builder()
                    .tableName(accountsTableName)
                    .key(Map.of(KEY_ACCOUNT_UUID, uuidAttr))
                    .updateExpression(
                        "SET #data = :data, #number = :number, #pni = :pni, #cds = :cds ADD #version :version_increment")
                    .conditionExpression(
                        "attribute_exists(#number) AND #version = :version")
                    .expressionAttributeNames(Map.of(
                        "#number", ATTR_ACCOUNT_E164,
                        "#data", ATTR_ACCOUNT_DATA,
                        "#cds", ATTR_CANONICALLY_DISCOVERABLE,
                        "#pni", ATTR_PNI_UUID,
                        "#version", ATTR_VERSION))
                    .expressionAttributeValues(Map.of(
                        ":number", numberAttr,
                        ":data", accountDataAttributeValue(account),
                        ":cds", AttributeValues.fromBool(account.isDiscoverableByPhoneNumber()),
                        ":pni", pniAttr,
                        ":version", AttributeValues.fromInt(account.getVersion()),
                        ":version_increment", AttributeValues.fromInt(1)))
                    .build())
                .build());

        writeItems.addAll(additionalWriteItems);

        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(writeItems)
            .build();

        dynamoDbClient.transactWriteItems(request);

        account.setVersion(account.getVersion() + 1);
        succeeded = true;
      } catch (final TransactionCanceledException e) {
        if (e.hasCancellationReasons()) {
          if (CONDITIONAL_CHECK_FAILED.equals(e.cancellationReasons().get(accountUpdateIndex).code())) {
            // the #version = :version condition failed, which indicates a concurrent update
            throw new ContestedOptimisticLockException();
          }
        } else {
          log.warn("Unexpected cancellation reasons: {}", e.cancellationReasons());

        }
        throw e;
      } finally {
        if (!succeeded) {
          account.setNumber(originalNumber, originalPni);
        }
      }
    });
  }


  /**
   * Reserve a username hash under the account UUID
   * @return a future that completes once the username hash has been reserved; may fail with an
   * {@link ContestedOptimisticLockException} if the account has been updated or there are concurrent updates to the
   * account or constraint records, and with an
   * {@link UsernameHashNotAvailableException} if the username was taken by someone else
   */
  public CompletableFuture<Void> reserveUsernameHash(
      final Account account,
      final byte[] reservedUsernameHash,
      final Duration ttl) {

    final Timer.Sample sample = Timer.start();

    // if there is an existing old reservation it will be cleaned up via ttl. Save it so we can restore it to the local
    // account if the update fails though.
    final Optional<byte[]> maybeOriginalReservation = account.getReservedUsernameHash();
    account.setReservedUsernameHash(reservedUsernameHash);

    // Normally when a username is reserved for the first time we reserve it for the provided TTL. But if the
    // reservation is for a username that we already have a reservation for (for example, if it's reclaimable, or there
    // is a hold) we might own that reservation for longer anyways, so we should preserve the original TTL in that case.
    // What we'd really like to do is set expirationTime = max(oldExpirationTime, now + ttl), but dynamodb doesn't
    // support that. Instead, we'll set expiration if it's greater than the existing expiration, otherwise retry
    final long expirationTime = clock.instant().plus(ttl).getEpochSecond();
    return tryReserveUsernameHash(account, reservedUsernameHash, expirationTime)
        .exceptionallyCompose(ExceptionUtils.exceptionallyHandler(TtlConflictException.class, ttlConflict ->
            // retry (once) with the returned expiration time
            tryReserveUsernameHash(account, reservedUsernameHash, ttlConflict.getExistingExpirationSeconds())))
        .whenComplete((response, throwable) -> {
          sample.stop(RESERVE_USERNAME_TIMER);

          if (throwable == null) {
            account.setVersion(account.getVersion() + 1);
          } else {
            account.setReservedUsernameHash(maybeOriginalReservation.orElse(null));
          }
        });
  }

  private static class TtlConflictException extends ContestedOptimisticLockException {
    private final long existingExpirationSeconds;
    TtlConflictException(final long existingExpirationSeconds) {
      super();
      this.existingExpirationSeconds = existingExpirationSeconds;
    }

    long getExistingExpirationSeconds() {
      return existingExpirationSeconds;
    }
  }

  /**
   * Try to reserve the provided usernameHash
   *
   * @param updatedAccount        The account, already updated to reserve the provided usernameHash
   * @param reservedUsernameHash  The usernameHash to reserve
   * @param expirationTimeSeconds When the reservation should expire
   * @return A future that completes successfully if the usernameHash was reserved
   * @throws TtlConflictException if the usernameHash was already reserved but with a longer TTL. The operation should
   *                              be retried with the returned {@link TtlConflictException#getExistingExpirationSeconds()}
   */
  private CompletableFuture<Void> tryReserveUsernameHash(
      final Account updatedAccount,
      final byte[] reservedUsernameHash,
      final long expirationTimeSeconds) {

    // Use account UUID as a "reservation token" - by providing this, the client proves ownership of the hash
    final UUID uuid = updatedAccount.getUuid();

    final List<TransactWriteItem> writeItems = new ArrayList<>();

    writeItems.add(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(usernamesConstraintTableName)
            .item(Map.of(
                UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(reservedUsernameHash),
                UsernameTable.ATTR_TTL, AttributeValues.fromLong(expirationTimeSeconds),
                UsernameTable.ATTR_CONFIRMED, AttributeValues.fromBool(false),
                UsernameTable.ATTR_RECLAIMABLE, AttributeValues.fromBool(false)))
            // we can make a reservation if no reservation exists for the name, or that reservation is expired, or there
            // is a reservation but it's ours and we haven't confirmed it yet and we're not accidentally reducing our
            // reservation's TTL. Note that confirmed=false => a TTL exists
            .conditionExpression("attribute_not_exists(#username_hash) OR #ttl < :now OR (#aci = :aci AND #confirmed = :false AND #ttl <= :expirationTime)")
            .expressionAttributeNames(Map.of(
                "#username_hash", UsernameTable.KEY_USERNAME_HASH,
                "#ttl", UsernameTable.ATTR_TTL,
                "#aci", UsernameTable.ATTR_ACCOUNT_UUID,
                "#confirmed", UsernameTable.ATTR_CONFIRMED))
            .expressionAttributeValues(Map.of(
                ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                ":aci", AttributeValues.fromUUID(uuid),
                ":false", AttributeValues.fromBool(false),
                ":expirationTime", AttributeValues.fromLong(expirationTimeSeconds)))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build());

    writeItems.add(UpdateAccountSpec.forAccount(accountsTableName, updatedAccount).transactItem());

    return dynamoDbAsyncClient
        .transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build())
        .thenRun(Util.NOOP)
        .exceptionally(ExceptionUtils.exceptionallyHandler(TransactionCanceledException.class, e -> {
          // If the constraint table update failed the condition check, the username's taken and we should stop
          // trying. However,
          if (conditionalCheckFailed(e.cancellationReasons().get(0))) {
            // The constraint table update failed the condition check. It could be because the username was taken,
            // or because we need to retry with a longer TTL
            final Map<String, AttributeValue> item = e.cancellationReasons().getFirst().item();
            final UUID existingOwner = AttributeValues.getUUID(item, UsernameTable.ATTR_ACCOUNT_UUID, null);
            final boolean confirmed = AttributeValues.getBool(item, UsernameTable.ATTR_CONFIRMED, false);
            final long existingTtl = AttributeValues.getLong(item, UsernameTable.ATTR_TTL, 0L);
            if (uuid.equals(existingOwner) && !confirmed && existingTtl > expirationTimeSeconds) {
              // We failed because we provided a shorter TTL than the one that exists on the reservation. The caller
              // can retry with updated expiration time.
              throw new TtlConflictException(existingTtl);
            }
            throw ExceptionUtils.wrap(new UsernameHashNotAvailableException());
          } else if (conditionalCheckFailed(e.cancellationReasons().get(1)) ||
              e.cancellationReasons().stream().anyMatch(Accounts::isTransactionConflict)) {
            // The accounts table fails the conditional check or either table was concurrently updated, it's an
            // optimistic locking failure and we should try again.
            throw new ContestedOptimisticLockException();
          } else {
            throw ExceptionUtils.wrap(e);
          }
        }));
  }

  /**
   * Add a held usernameHash to the account object.
   * <p>
   * An account may only have up to MAX_USERNAME_HOLDS held usernames. If adding this hold pushes the account over this
   * limit, a usernameHash is returned that the caller must release their hold on.
   * <p>
   * This only tracks the holds associated with the account, ensuring that no other account can take a held username is
   * done via the username constraint table, and should be done transactionally with writing the updated account.
   *
   * @param accountToUpdate The account to update (in-place)
   * @param newHold         A username hash to add to the account's holds
   * @param now             The current time
   * @return If present, an old hold that the caller should remove from the username constraint table
   */
  private Optional<byte[]> addToHolds(final Account accountToUpdate, final byte[] newHold, final Instant now) {
    List<Account.UsernameHold> holds = new ArrayList<>(accountToUpdate.getUsernameHolds());
    final Account.UsernameHold holdToAdd = new Account.UsernameHold(newHold,
        now.plus(USERNAME_HOLD_DURATION).getEpochSecond());

    // Remove any holds that are
    // - expired
    // - match what we're trying to add (we'll re-add it at the end of the list to refresh the ttl)
    // - match our current username
    holds.removeIf(hold -> hold.expirationSecs() < now.getEpochSecond()
        || Arrays.equals(newHold, hold.usernameHash())
        || accountToUpdate.getUsernameHash().map(curr -> Arrays.equals(curr, hold.usernameHash())).orElse(false));

    // add the new hold
    holds.add(holdToAdd);

    if (holds.size() <= MAX_USERNAME_HOLDS) {
      accountToUpdate.setUsernameHolds(holds);
      Metrics.counter(USERNAME_HOLD_ADDED_COUNTER_NAME, "max", String.valueOf(false)).increment();
      return Optional.empty();
    } else {
      accountToUpdate.setUsernameHolds(holds.subList(1, holds.size()));
      Metrics.counter(USERNAME_HOLD_ADDED_COUNTER_NAME, "max", String.valueOf(true)).increment();
      // Newer holds are always added to the end of the holds list, so the first hold is always the oldest hold. Note
      // that if a duplicate hold is added, we remove it from the list and re-add it at the end, this preserves hold
      // ordering
      return Optional.of(holds.getFirst().usernameHash());
    }
  }

  /**
   * Transaction item to update the usernameConstraintTable to "hold" a usernameHash for an account
   *
   * @param holder       The account with the hold.
   * @param usernameHash The hash to reserve for the account
   * @param now          The current time
   * @return A transaction item that will update the usernameConstraintTable.
   */
  private TransactWriteItem holdUsernameTransactItem(final UUID holder, final byte[] usernameHash, final Instant now) {
    return TransactWriteItem.builder().put(Put.builder()
        .tableName(usernamesConstraintTableName)
        .item(Map.of(
            UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
            UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(holder),
            UsernameTable.ATTR_CONFIRMED, AttributeValues.fromBool(false),
            UsernameTable.ATTR_TTL,
            AttributeValues.fromLong(now.plus(USERNAME_HOLD_DURATION).getEpochSecond())))
        .build()).build();
  }

  /**
   * Transaction item to release a hold on the usernameConstraintTable
   *
   * @param holder                The account with the hold.
   * @param usernameHashToRelease The hash to release for the account
   * @param now                   The current time
   * @return A transaction item that will update the usernameConstraintTable. The transaction will fail with a condition
   * exception if someone else has a reservation for usernameHashToRelease
   */
  private TransactWriteItem releaseHoldIfAllowedTransactItem(
      final UUID holder, final byte[] usernameHashToRelease, final Instant now) {
    return TransactWriteItem.builder().delete(Delete.builder()
        .tableName(usernamesConstraintTableName)
        .key(Map.of(UsernameTable.KEY_USERNAME_HASH, AttributeValues.b(usernameHashToRelease)))
        // we can release the hold if we own it (and it's not our confirmed username) or if no one owns it
        .conditionExpression("(#aci = :aci AND #confirmed = :false) OR #ttl < :now OR attribute_not_exists(#usernameHash)")
        .expressionAttributeNames(Map.of(
            "#usernameHash", UsernameTable.KEY_USERNAME_HASH,
            "#aci", UsernameTable.ATTR_ACCOUNT_UUID,
            "#confirmed", UsernameTable.ATTR_CONFIRMED,
            "#ttl", UsernameTable.ATTR_TTL))
        .expressionAttributeValues(Map.of(
            ":aci", AttributeValues.b(holder),
            ":now", AttributeValues.n(now.getEpochSecond()),
            ":false", AttributeValues.fromBool(false)))
        .build()).build();
  }

  /**
   * Confirm (set) a previously reserved username hash
   *
   * @param account to update
   * @param usernameHash believed to be available
   * @param encryptedUsername the encrypted form of the previously reserved username; used for the username link
   * @return a future that completes once the username hash has been confirmed; may fail with an
   * {@link ContestedOptimisticLockException} if the account has been updated or there are concurrent updates to the
   * account or constraint records, and with an
   * {@link UsernameHashNotAvailableException} if the username was taken by someone else
   */
  public CompletableFuture<Void> confirmUsernameHash(final Account account, final byte[] usernameHash, @Nullable final byte[] encryptedUsername) {
    final Timer.Sample sample = Timer.start();
    if (usernameHash == null) {
      throw new IllegalArgumentException("Cannot confirm a null usernameHash");
    }

    return pickLinkHandle(account, usernameHash)
        .thenCompose(linkHandle -> {
          final Optional<byte[]> maybeOriginalUsernameHash = account.getUsernameHash();
          final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
          updatedAccount.setUsernameHash(usernameHash);
          updatedAccount.setReservedUsernameHash(null);
          updatedAccount.setUsernameLinkDetails(encryptedUsername == null ? null : linkHandle, encryptedUsername);
          final Instant now = clock.instant();
          final Optional<byte[]> holdToRemove = maybeOriginalUsernameHash
              .flatMap(hold -> addToHolds(updatedAccount, hold, now));

          final List<TransactWriteItem> writeItems = new ArrayList<>();

          // 0: add the username hash to the constraint table, wiping out the ttl if we had already reserved the hash
          writeItems.add(TransactWriteItem.builder().put(Put.builder()
                  .tableName(usernamesConstraintTableName)
                  .item(Map.of(
                      UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid()),
                      UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
                      UsernameTable.ATTR_CONFIRMED, AttributeValues.fromBool(true)))
                  // it's not in the constraint table OR it's expired OR it was reserved by us
                  .conditionExpression("attribute_not_exists(#username_hash) OR #ttl < :now OR (#aci = :aci AND #confirmed = :confirmed)")
                  .expressionAttributeNames(Map.of(
                      "#username_hash", UsernameTable.KEY_USERNAME_HASH,
                      "#ttl", UsernameTable.ATTR_TTL,
                      "#aci", UsernameTable.ATTR_ACCOUNT_UUID,
                      "#confirmed", UsernameTable.ATTR_CONFIRMED))
                  .expressionAttributeValues(Map.of(
                      ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                      ":aci", AttributeValues.fromUUID(updatedAccount.getUuid()),
                      ":confirmed", AttributeValues.fromBool(false)))
                  .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                  .build())
              .build());

          // 1: update the account object (conditioned on the version increment)
          writeItems.add(UpdateAccountSpec.forAccount(accountsTableName, updatedAccount).transactItem());

          // 2?: Add a temporary hold for the old username to stop others from claiming it
          maybeOriginalUsernameHash.ifPresent(originalUsernameHash ->
              writeItems.add(holdUsernameTransactItem(updatedAccount.getUuid(), originalUsernameHash, now)));

          // 3?: Adding that hold may have caused our account to exceed our maximum holds. Release an old hold
          holdToRemove.ifPresent(oldHold ->
              writeItems.add(releaseHoldIfAllowedTransactItem(updatedAccount.getUuid(), oldHold, now)));

          return dynamoDbAsyncClient.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build())
              .thenApply(ignored -> updatedAccount);
        })
        .thenApply(updatedAccount -> {
          account.setUsernameHash(usernameHash);
          account.setReservedUsernameHash(null);
          account.setUsernameLinkDetails(updatedAccount.getUsernameLinkHandle(), updatedAccount.getEncryptedUsername().orElse(null));
          account.setUsernameHolds(updatedAccount.getUsernameHolds());
          account.setVersion(account.getVersion() + 1);
          return (Void) null;
        })
        .exceptionally(ExceptionUtils.exceptionallyHandler(TransactionCanceledException.class, e -> {
          // If the constraint table update failed the condition check, the username's taken and we should stop
          // trying. However, if the accounts table fails the conditional check or either table was concurrently
          // updated, it's an optimistic locking failure and we should try again.
          if (conditionalCheckFailed(e.cancellationReasons().get(0))) {
            throw ExceptionUtils.wrap(new UsernameHashNotAvailableException());
          } else if (conditionalCheckFailed(e.cancellationReasons().get(1)) // Account version conflict
              // When we looked at the holds on our account, we thought we still held the corresponding username
              // reservation. But it turned out that someone else has taken the reservation since. This means that the
              // TTL on the hold must have just expired, so if we retry we should see that our hold is expired, and we
              // won't try to remove it again.
              || (e.cancellationReasons().size() > 3 && conditionalCheckFailed(e.cancellationReasons().get(3)))
              // concurrent update on any table
              || e.cancellationReasons().stream().anyMatch(Accounts::isTransactionConflict)) {
            throw new ContestedOptimisticLockException();
          } else {
            throw ExceptionUtils.wrap(e);
          }
        }))
        .whenComplete((ignored, throwable) -> sample.stop(SET_USERNAME_TIMER));
  }

  private CompletableFuture<UUID> pickLinkHandle(final Account account, final byte[] usernameHash) {
    if (account.getUsernameLinkHandle() == null) {
      // There's no old link handle, so we can just use a randomly generated link handle
      return CompletableFuture.completedFuture(UUID.randomUUID());
    }

    // Otherwise, there's an existing link handle. If this is the result of an account being re-registered, we should
    // preserve the link handle.
    return dynamoDbAsyncClient.getItem(GetItemRequest.builder()
            .tableName(usernamesConstraintTableName)
            .key(Map.of(UsernameTable.KEY_USERNAME_HASH, AttributeValues.b(usernameHash)))
            .projectionExpression(UsernameTable.ATTR_RECLAIMABLE).build())
        .thenApply(response -> {
          if (response.hasItem() && AttributeValues.getBool(response.item(), UsernameTable.ATTR_RECLAIMABLE, false)) {
            // this username reservation indicates it's a username waiting to be "reclaimed"
            return account.getUsernameLinkHandle();
          }
          // There was no existing username reservation, or this was a standard "new" username. Either way, we should
          // generate a new link handle.
          return UUID.randomUUID();
        });
  }

  /**
   * Clear the username hash and link from the given account
   *
   * @param account to update
   * @return a future that completes once the username data has been cleared;
   * it can fail with a {@link ContestedOptimisticLockException} if there are concurrent updates
   * to the account or username constraint records.
   */
  public CompletableFuture<Void> clearUsernameHash(final Account account) {
    if (account.getUsernameHash().isEmpty()) {
      // no username to clear
      return CompletableFuture.completedFuture(null);
    }
    final byte[] usernameHash = account.getUsernameHash().get();
    final Timer.Sample sample = Timer.start();

    final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
    updatedAccount.setUsernameHash(null);
    updatedAccount.setUsernameLinkDetails(null, null);

    final Instant now = clock.instant();
    final Optional<byte[]> holdToRemove = addToHolds(updatedAccount, usernameHash, now);

    final List<TransactWriteItem> items = new ArrayList<>();

    // 0: remove the username from the account object, conditioned on account version
    items.add(UpdateAccountSpec.forAccount(accountsTableName, updatedAccount).transactItem());

    // 1: Un-confirm our username, adding a temporary hold for the old username to stop others from claiming it
    items.add(holdUsernameTransactItem(updatedAccount.getUuid(), usernameHash, now));

    // 2?: Adding that hold may have caused our account to exceed our maximum holds. Release an old hold
    holdToRemove.ifPresent(oldHold -> items.add(releaseHoldIfAllowedTransactItem(updatedAccount.getUuid(), oldHold, now)));

    return dynamoDbAsyncClient.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(items).build())
        .thenAccept(ignored -> {
          account.setUsernameHash(null);
          account.setUsernameLinkDetails(null, null);
          account.setVersion(account.getVersion() + 1);
          account.setUsernameHolds(updatedAccount.getUsernameHolds());
        })
        .exceptionally(ExceptionUtils.exceptionallyHandler(TransactionCanceledException.class, e -> {
          if (conditionalCheckFailed(e.cancellationReasons().get(0)) // Account version conflict
              // When we looked at the holds on our account, we thought we still held the corresponding username
              // reservation. But it turned out that someone else has taken the reservation since. This means that the
              // TTL on the hold must have just expired, so if we retry we should see that our hold is expired, and we
              // won't try to remove it again.
              || (e.cancellationReasons().size() > 2 && conditionalCheckFailed(e.cancellationReasons().get(2)))
              // concurrent update on any table
              || e.cancellationReasons().stream().anyMatch(Accounts::isTransactionConflict)) {
            throw new ContestedOptimisticLockException();
          } else {
            throw ExceptionUtils.wrap(e);
          }
        }))
        .whenComplete((ignored, throwable) -> sample.stop(CLEAR_USERNAME_HASH_TIMER));
  }

  /**
   * A ddb update that can be used as part of a transaction or single-item update statement.
   */
  record UpdateAccountSpec(
      String tableName,
      Map<String, AttributeValue> key,
      Map<String, String> attrNames,
      Map<String, AttributeValue> attrValues,
      String updateExpression,
      String conditionExpression) {
    UpdateItemRequest updateItemRequest() {
      return UpdateItemRequest.builder()
          .tableName(tableName)
          .key(key)
          .updateExpression(updateExpression)
          .conditionExpression(conditionExpression)
          .expressionAttributeNames(attrNames)
          .expressionAttributeValues(attrValues)
          .build();
    }

    TransactWriteItem transactItem() {
      return TransactWriteItem.builder().update(Update.builder()
          .tableName(tableName)
          .key(key)
          .updateExpression(updateExpression)
          .conditionExpression(conditionExpression)
          .expressionAttributeNames(attrNames)
          .expressionAttributeValues(attrValues)
          .build()).build();
    }

    static UpdateAccountSpec forAccount(
        final String accountTableName,
        final Account account) {
      // username, e164, and pni cannot be modified through this method
      final Map<String, String> attrNames = new HashMap<>(Map.of(
          "#number", ATTR_ACCOUNT_E164,
          "#data", ATTR_ACCOUNT_DATA,
          "#cds", ATTR_CANONICALLY_DISCOVERABLE,
          "#version", ATTR_VERSION));

      final Map<String, AttributeValue> attrValues = new HashMap<>(Map.of(
          ":data", accountDataAttributeValue(account),
          ":cds", AttributeValues.fromBool(account.isDiscoverableByPhoneNumber()),
          ":version", AttributeValues.fromInt(account.getVersion()),
          ":version_increment", AttributeValues.fromInt(1)));

      final StringBuilder updateExpressionBuilder = new StringBuilder("SET #data = :data, #cds = :cds");
      if (account.getUnidentifiedAccessKey().isPresent()) {
        // if it's present in the account, also set the uak
        attrNames.put("#uak", ATTR_UAK);
        attrValues.put(":uak", AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
        updateExpressionBuilder.append(", #uak = :uak");
      }

      if (account.getUsernameHash().isPresent()) {
        // if it's present in the account, also set the username hash
        attrNames.put("#usernameHash", ATTR_USERNAME_HASH);
        attrValues.put(":usernameHash", AttributeValues.fromByteArray(account.getUsernameHash().get()));
        updateExpressionBuilder.append(", #usernameHash = :usernameHash");
      }

      // If the account has a username/handle pair, we should add it to the top level attributes.
      // When we remove an encryptedUsername but preserve the link (re-registration), it's possible that the account
      // has a usernameLinkHandle but not an encrypted username. In this case there should already be a top-level
      // usernameLink attribute.
      if (account.getEncryptedUsername().isPresent() && account.getUsernameLinkHandle() != null) {
        attrNames.put("#ul", ATTR_USERNAME_LINK_UUID);
        attrValues.put(":ul", AttributeValues.fromUUID(account.getUsernameLinkHandle()));
        updateExpressionBuilder.append(", #ul = :ul");
      }

      // Some operations may remove the usernameLink or the usernameHash (re-registration, clear username link, and
      // clear username hash). Since these also have top-level ddb attributes, we need to make sure to remove those
      // as well.
      final List<String> removes = new ArrayList<>();
      if (account.getUsernameLinkHandle() == null) {
        attrNames.put("#ul", ATTR_USERNAME_LINK_UUID);
        removes.add("#ul");
      }
      if (account.getUsernameHash().isEmpty()) {
        attrNames.put("#username_hash", ATTR_USERNAME_HASH);
        removes.add("#username_hash");
      }
      if (!removes.isEmpty()) {
        updateExpressionBuilder.append(" REMOVE %s".formatted(String.join(",", removes)));
      }
      updateExpressionBuilder.append(" ADD #version :version_increment");

      return new UpdateAccountSpec(
          accountTableName,
          Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())),
          attrNames,
          attrValues,
          updateExpressionBuilder.toString(),
          "attribute_exists(#number) AND #version = :version");
    }
  }

  @Nonnull
  public CompletionStage<Void> updateAsync(final Account account) {
    return AsyncTimerUtil.record(UPDATE_TIMER, () -> {
      final UpdateItemRequest updateItemRequest = UpdateAccountSpec
          .forAccount(accountsTableName, account)
          .updateItemRequest();

      return dynamoDbAsyncClient.updateItem(updateItemRequest)
          .thenApply(response -> {
            account.setVersion(AttributeValues.getInt(response.attributes(), "V", account.getVersion() + 1));
            return (Void) null;
          })
          .exceptionallyCompose(throwable -> {
            final Throwable unwrapped = ExceptionUtils.unwrap(throwable);
            if (unwrapped instanceof TransactionConflictException) {
              throw new ContestedOptimisticLockException();
            } else if (unwrapped instanceof ConditionalCheckFailedException e) {
              // the exception doesn't give details about which condition failed,
              // but we can infer it was an optimistic locking failure if the UUID is known
              return getByAccountIdentifierAsync(account.getUuid())
                  .thenAccept(refreshedAccount -> {
                    throw refreshedAccount.isPresent() ? new ContestedOptimisticLockException() : e;
                  });
            } else {
              // rethrow
              throw CompletableFutureUtils.errorAsCompletionException(throwable);
            }
          });
    });
  }

  private static void joinAndUnwrapUpdateFuture(CompletionStage<Void> future) {
    try {
      future.toCompletableFuture().join();
    } catch (final CompletionException e) {
      // unwrap CompletionExceptions, throw as long is it's unchecked
      Throwables.throwIfUnchecked(ExceptionUtils.unwrap(e));

      // if we otherwise somehow got a wrapped checked exception,
      // rethrow the checked exception wrapped by the original CompletionException
      log.error("Unexpected checked exception thrown from dynamo update", e);
      throw e;
    }
  }

  public void update(final Account account) throws ContestedOptimisticLockException {
    joinAndUnwrapUpdateFuture(updateAsync(account));
  }

  public CompletionStage<Void> updateTransactionallyAsync(final Account account,
      final Collection<TransactWriteItem> additionalWriteItems) {

    return AsyncTimerUtil.record(UPDATE_TRANSACTIONALLY_TIMER, () -> {
      final List<TransactWriteItem> writeItems = new ArrayList<>(additionalWriteItems.size() + 1);
      writeItems.add(UpdateAccountSpec.forAccount(accountsTableName, account).transactItem());
      writeItems.addAll(additionalWriteItems);

      return dynamoDbAsyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
              .transactItems(writeItems)
              .build())
          .thenApply(response -> {
            account.setVersion(account.getVersion() + 1);
            return (Void) null;
          })
          .exceptionally(throwable -> {
            final Throwable unwrapped = ExceptionUtils.unwrap(throwable);

            if (unwrapped instanceof TransactionCanceledException transactionCanceledException) {
              if (CONDITIONAL_CHECK_FAILED.equals(transactionCanceledException.cancellationReasons().getFirst().code())) {
                throw new ContestedOptimisticLockException();
              }

              if (transactionCanceledException.cancellationReasons()
                  .stream()
                  .anyMatch(reason -> TRANSACTION_CONFLICT.equals(reason.code()))) {

                throw new ContestedOptimisticLockException();
              }
            }

            throw CompletableFutureUtils.errorAsCompletionException(throwable);
          });
    });
  }

  public TransactWriteItem buildTransactWriteItemForLinkDevice(final String linkDeviceToken, final Duration tokenTtl) {
    final byte[] linkDeviceTokenHash;

    try {
      linkDeviceTokenHash = MessageDigest.getInstance("SHA-256").digest(linkDeviceToken.getBytes(StandardCharsets.UTF_8));
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support the SHA-256 MessageDigest algorithm", e);
    }

    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(usedLinkDeviceTokenTableName)
            .item(Map.of(
                KEY_LINK_DEVICE_TOKEN_HASH, AttributeValue.fromB(SdkBytes.fromByteArray(linkDeviceTokenHash)),
                ATTR_LINK_DEVICE_TOKEN_TTL, AttributeValue.fromN(String.valueOf(clock.instant().plus(tokenTtl).getEpochSecond()))
            ))
            .conditionExpression("attribute_not_exists(#linkDeviceTokenHash)")
            .expressionAttributeNames(Map.of("#linkDeviceTokenHash", KEY_LINK_DEVICE_TOKEN_HASH))
            .build())
        .build();
  }

  @Nonnull
  public Optional<Account> getByE164(final String number) {
    return getByIndirectLookup(
        GET_BY_NUMBER_TIMER, phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, AttributeValues.fromString(number));
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByE164Async(final String number) {
    return getByIndirectLookupAsync(
        GET_BY_NUMBER_TIMER, phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, AttributeValues.fromString(number));
  }

  @Nonnull
  public Optional<Account> getByPhoneNumberIdentifier(final UUID phoneNumberIdentifier) {
    return getByIndirectLookup(
        GET_BY_PNI_TIMER, phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier));
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByPhoneNumberIdentifierAsync(final UUID phoneNumberIdentifier) {
    return getByIndirectLookupAsync(GET_BY_PNI_TIMER, phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier));
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByUsernameHash(final byte[] usernameHash) {
    return getByIndirectLookupAsync(GET_BY_USERNAME_HASH_TIMER,
        usernamesConstraintTableName,
        UsernameTable.KEY_USERNAME_HASH,
        AttributeValues.fromByteArray(usernameHash),
        item -> AttributeValues.getBool(item, UsernameTable.ATTR_CONFIRMED, false) // ignore items that are reservations (not confirmed)
    );
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByUsernameLinkHandle(final UUID usernameLinkHandle) {
    final Timer.Sample sample = Timer.start();

    return itemByGsiKeyAsync(accountsTableName, USERNAME_LINK_TO_UUID_INDEX, ATTR_USERNAME_LINK_UUID, AttributeValues.fromUUID(usernameLinkHandle))
        .thenApply(maybeItem -> maybeItem.map(Accounts::fromItem))
        .whenComplete((account, throwable) -> sample.stop(GET_BY_USERNAME_LINK_HANDLE_TIMER));
  }

  @Nonnull
  public Optional<Account> getByAccountIdentifier(final UUID uuid) {
    return requireNonNull(GET_BY_UUID_TIMER.record(() ->
        itemByKey(accountsTableName, KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))
            .map(Accounts::fromItem)));
  }

  private TransactWriteItem buildPutDeletedAccount(final UUID aci, final UUID pni) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(deletedAccountsTableName)
            .item(Map.of(
                DELETED_ACCOUNTS_KEY_ACCOUNT_PNI, AttributeValues.fromString(pni.toString()),
                DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(aci),
                DELETED_ACCOUNTS_ATTR_EXPIRES, AttributeValues.fromLong(clock.instant().plus(DELETED_ACCOUNTS_TIME_TO_LIVE).getEpochSecond())))
            .build())
        .build();
  }

  private TransactWriteItem buildRemoveDeletedAccount(final UUID pni) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(deletedAccountsTableName)
            .key(Map.of(DELETED_ACCOUNTS_KEY_ACCOUNT_PNI, AttributeValues.fromString(pni.toString())))
            .build())
        .build();
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByAccountIdentifierAsync(final UUID uuid) {
    return AsyncTimerUtil.record(GET_BY_UUID_TIMER, () -> itemByKeyAsync(accountsTableName, KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))
        .thenApply(maybeItem -> maybeItem.map(Accounts::fromItem)))
        .toCompletableFuture();
  }

  public Optional<UUID> findRecentlyDeletedAccountIdentifier(final UUID phoneNumberIdentifier) {
    final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(deletedAccountsTableName)
        .consistentRead(true)
        .key(Map.of(DELETED_ACCOUNTS_KEY_ACCOUNT_PNI, AttributeValues.fromString(phoneNumberIdentifier.toString())))
        .build());

    return Optional.ofNullable(AttributeValues.getUUID(response.item(), DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID, null));
  }

  public Optional<UUID> findRecentlyDeletedPhoneNumberIdentifier(final UUID uuid) {
    final QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
        .tableName(deletedAccountsTableName)
        .indexName(DELETED_ACCOUNTS_UUID_TO_PNI_INDEX_NAME)
        .keyConditionExpression("#uuid = :uuid")
        .projectionExpression("#pni")
        .expressionAttributeNames(Map.of("#uuid", DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID,
            "#pni", DELETED_ACCOUNTS_KEY_ACCOUNT_PNI))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid))).build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    return response.items().stream()
        .map(item -> item.get(DELETED_ACCOUNTS_KEY_ACCOUNT_PNI).s())
        .filter(e164OrPni -> !e164OrPni.startsWith("+"))
        .findFirst()
        .map(UUID::fromString);
  }

  public CompletableFuture<Void> delete(final UUID uuid, final List<TransactWriteItem> additionalWriteItems) {
    final Timer.Sample sample = Timer.start();

    return getByAccountIdentifierAsync(uuid)
        .thenCompose(maybeAccount -> maybeAccount.map(account -> {
              final List<TransactWriteItem> transactWriteItems = new ArrayList<>(List.of(
                  buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, account.getNumber()),
                  buildDelete(accountsTableName, KEY_ACCOUNT_UUID, uuid),
                  buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, account.getPhoneNumberIdentifier()),
                  buildPutDeletedAccount(uuid, account.getPhoneNumberIdentifier())
              ));

              account.getUsernameHash().ifPresent(usernameHash -> transactWriteItems.add(
                  buildDelete(usernamesConstraintTableName, UsernameTable.KEY_USERNAME_HASH, usernameHash)));

              transactWriteItems.addAll(additionalWriteItems);

              return dynamoDbAsyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
                  .transactItems(transactWriteItems)
                  .build())
                  .thenRun(Util.NOOP);
            })
            .orElseGet(() -> CompletableFuture.completedFuture(null)))
            .thenRun(() -> sample.stop(DELETE_TIMER));
  }

  Flux<Account> getAll(final int segments, final Scheduler scheduler) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> dynamoDbAsyncClient.scanPaginator(ScanRequest.builder()
                .tableName(accountsTableName)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .build())
            .items()
            .map(Accounts::fromItem))
        .sequential();
  }

  Flux<UUID> getAllAccountIdentifiers(final int segments, final Scheduler scheduler) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> dynamoDbAsyncClient.scanPaginator(ScanRequest.builder()
                .tableName(accountsTableName)
                .consistentRead(false)
                .segment(segment)
                .totalSegments(segments)
                .projectionExpression(KEY_ACCOUNT_UUID)
                .build())
            .items()
            .map(item -> AttributeValues.getUUID(item, KEY_ACCOUNT_UUID, null)))
        .sequential();
  }

  @Nonnull
  private Optional<Account> getByIndirectLookup(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue) {
    return getByIndirectLookup(timer, tableName, keyName, keyValue, i -> true);
  }

  @Nonnull
  private CompletableFuture<Optional<Account>> getByIndirectLookupAsync(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue) {

    return getByIndirectLookupAsync(timer, tableName, keyName, keyValue, i -> true);
  }

  @Nonnull
  private Optional<Account> getByIndirectLookup(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue,
      final Predicate<? super Map<String, AttributeValue>> predicate) {

    return requireNonNull(timer.record(() -> itemByKey(tableName, keyName, keyValue)
        .filter(predicate)
        .map(item -> item.get(KEY_ACCOUNT_UUID))
        .flatMap(uuid -> itemByKey(accountsTableName, KEY_ACCOUNT_UUID, uuid))
        .map(Accounts::fromItem)));
  }

  @Nonnull
  private CompletableFuture<Optional<Account>> getByIndirectLookupAsync(
      final Timer timer,
      final String tableName,
      final String keyName,
      final AttributeValue keyValue,
      final Predicate<? super Map<String, AttributeValue>> predicate) {

    return AsyncTimerUtil.record(timer, () -> itemByKeyAsync(tableName, keyName, keyValue)
        .thenCompose(maybeItem -> maybeItem
            .filter(predicate)
            .map(item -> item.get(KEY_ACCOUNT_UUID))
            .map(uuid -> itemByKeyAsync(accountsTableName, KEY_ACCOUNT_UUID, uuid)
                .thenApply(maybeAccountItem -> maybeAccountItem.map(Accounts::fromItem)))
            .orElse(CompletableFuture.completedFuture(Optional.empty()))))
        .toCompletableFuture();
  }

  @Nonnull
  private Optional<Map<String, AttributeValue>> itemByKey(final String table, final String keyName, final AttributeValue keyValue) {
    final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(table)
        .key(Map.of(keyName, keyValue))
        .consistentRead(true)
        .build());
    return Optional.ofNullable(response.item()).filter(m -> !m.isEmpty());
  }

  @Nonnull
  private CompletableFuture<Optional<Map<String, AttributeValue>>> itemByKeyAsync(final String table, final String keyName, final AttributeValue keyValue) {
    return dynamoDbAsyncClient.getItem(GetItemRequest.builder()
            .tableName(table)
            .key(Map.of(keyName, keyValue))
            .consistentRead(true)
            .build())
        .thenApply(response -> Optional.ofNullable(response.item()).filter(item -> !item.isEmpty()));
  }

  @Nonnull
  private CompletableFuture<Optional<Map<String, AttributeValue>>> itemByGsiKeyAsync(final String table, final String indexName, final String keyName, final AttributeValue keyValue) {
    return dynamoDbAsyncClient.query(QueryRequest.builder()
        .tableName(table)
        .indexName(indexName)
        .keyConditionExpression("#gsiKey = :gsiValue")
        .projectionExpression("#uuid")
        .expressionAttributeNames(Map.of(
            "#gsiKey", keyName,
            "#uuid", KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(
            ":gsiValue", keyValue))
        .build())
        .thenCompose(response -> {
          if (response.count() == 0) {
            return CompletableFuture.completedFuture(Optional.empty());
          }

          if (response.count() > 1) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "More than one row located for GSI [%s], key-value pair [%s, %s]"
                    .formatted(indexName, keyName, keyValue)));
          }

          final AttributeValue primaryKeyValue = response.items().getFirst().get(KEY_ACCOUNT_UUID);
          return itemByKeyAsync(table, KEY_ACCOUNT_UUID, primaryKeyValue);
        });
  }

  @Nonnull
  private TransactWriteItem buildAccountPut(
      final Account account,
      final AttributeValue uuidAttr,
      final AttributeValue numberAttr,
      final AttributeValue pniUuidAttr) {

    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        KEY_ACCOUNT_UUID, uuidAttr,
        ATTR_ACCOUNT_E164, numberAttr,
        ATTR_PNI_UUID, pniUuidAttr,
        ATTR_ACCOUNT_DATA, accountDataAttributeValue(account),
        ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
        ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.isDiscoverableByPhoneNumber())));

    // Add the UAK if it's in the account
    account.getUnidentifiedAccessKey()
        .map(AttributeValues::fromByteArray)
        .ifPresent(uak -> item.put(ATTR_UAK, uak));

    return TransactWriteItem.builder()
        .put(Put.builder()
            .conditionExpression("attribute_not_exists(#pni) OR #pni = :pni")
            .expressionAttributeNames(Map.of("#pni", ATTR_PNI_UUID))
            .expressionAttributeValues(Map.of(":pni", pniUuidAttr))
            .tableName(accountsTableName)
            .item(item)
            .build())
        .build();
  }

  @Nonnull
  private static TransactWriteItem buildConstraintTablePutIfAbsent(
      final String tableName,
      final AttributeValue uuidAttr,
      final String keyName,
      final AttributeValue keyValue) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(tableName)
            .item(Map.of(
                keyName, keyValue,
                KEY_ACCOUNT_UUID, uuidAttr))
            .conditionExpression(
                "attribute_not_exists(#key) OR #uuid = :uuid")
            .expressionAttributeNames(Map.of(
                "#key", keyName,
                "#uuid", KEY_ACCOUNT_UUID))
            .expressionAttributeValues(Map.of(
                ":uuid", uuidAttr))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build();
  }

  @Nonnull
  private static TransactWriteItem buildConstraintTablePut(
      final String tableName,
      final AttributeValue uuidAttr,
      final String keyName,
      final AttributeValue keyValue) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(tableName)
            .item(Map.of(
                keyName, keyValue,
                KEY_ACCOUNT_UUID, uuidAttr))
            .conditionExpression(
                "attribute_not_exists(#key)")
            .expressionAttributeNames(Map.of(
                "#key", keyName))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build();
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final String keyValue) {
    return buildDelete(tableName, keyName, AttributeValues.fromString(keyValue));
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final byte[] keyValue) {
    return buildDelete(tableName, keyName, AttributeValues.fromByteArray(keyValue));
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final UUID keyValue) {
    return buildDelete(tableName, keyName, AttributeValues.fromUUID(keyValue));
  }

  @Nonnull
  private static TransactWriteItem buildDelete(final String tableName, final String keyName, final AttributeValue keyValue) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(tableName)
            .key(Map.of(keyName, keyValue))
            .build())
        .build();
  }

  CompletableFuture<Void> regenerateConstraints(final Account account) {
    final List<CompletableFuture<?>> constraintFutures = new ArrayList<>();

    constraintFutures.add(writeConstraint(phoneNumberConstraintTableName,
        account.getIdentifier(IdentityType.ACI),
        ATTR_ACCOUNT_E164,
        AttributeValues.fromString(account.getNumber())));

    constraintFutures.add(writeConstraint(phoneNumberIdentifierConstraintTableName,
        account.getIdentifier(IdentityType.ACI),
        ATTR_PNI_UUID,
        AttributeValues.fromUUID(account.getPhoneNumberIdentifier())));

    account.getUsernameHash().ifPresent(usernameHash ->
        constraintFutures.add(writeUsernameConstraint(account.getIdentifier(IdentityType.ACI),
            usernameHash,
            Optional.empty())));

    account.getUsernameHolds().forEach(usernameHold ->
        constraintFutures.add(writeUsernameConstraint(account.getIdentifier(IdentityType.ACI),
            usernameHold.usernameHash(),
            Optional.of(Instant.ofEpochSecond(usernameHold.expirationSecs())))));

    return CompletableFuture.allOf(constraintFutures.toArray(CompletableFuture[]::new));
  }

  private CompletableFuture<Void> writeConstraint(
      final String tableName,
      final UUID accountIdentifier,
      final String keyName,
      final AttributeValue keyValue) {

    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(Map.of(
                keyName, keyValue,
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier)))
        .build())
        .thenRun(Util.NOOP);
  }

  private CompletableFuture<Void> writeUsernameConstraint(
      final UUID accountIdentifier,
      final byte[] usernameHash,
      final Optional<Instant> maybeExpiration) {

    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        UsernameTable.KEY_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
        UsernameTable.ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(accountIdentifier),
        UsernameTable.ATTR_CONFIRMED, AttributeValues.fromBool(maybeExpiration.isEmpty())
    ));

    maybeExpiration.ifPresent(expiration ->
        item.put(UsernameTable.ATTR_TTL, AttributeValues.fromLong(expiration.getEpochSecond())));

    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(usernamesConstraintTableName)
            .item(item)
        .build())
        .thenRun(Util.NOOP);
  }

  @Nonnull
  private static String extractCancellationReasonCodes(final TransactionCanceledException exception) {
    return exception.cancellationReasons().stream()
        .map(CancellationReason::code)
        .collect(Collectors.joining(", "));
  }

  @VisibleForTesting
  @Nonnull
  static Account fromItem(final Map<String, AttributeValue> item) {
    if (!item.containsKey(ATTR_ACCOUNT_DATA)
        || !item.containsKey(ATTR_ACCOUNT_E164)
        || !item.containsKey(KEY_ACCOUNT_UUID)
        || !item.containsKey(ATTR_CANONICALLY_DISCOVERABLE)) {
      throw new RuntimeException("item missing values");
    }
    try {
      final Account account = SystemMapper.jsonMapper().readValue(item.get(ATTR_ACCOUNT_DATA).b().asByteArray(), Account.class);

      final UUID accountIdentifier = UUIDUtil.fromByteBuffer(item.get(KEY_ACCOUNT_UUID).b().asByteBuffer());
      final UUID phoneNumberIdentifierFromAttribute = AttributeValues.getUUID(item, ATTR_PNI_UUID, null);

      if (account.getPhoneNumberIdentifier() == null || phoneNumberIdentifierFromAttribute == null ||
          !Objects.equals(account.getPhoneNumberIdentifier(), phoneNumberIdentifierFromAttribute)) {

        log.warn("Missing or mismatched PNIs for account {}. From JSON: {}; from attribute: {}",
            accountIdentifier, account.getPhoneNumberIdentifier(), phoneNumberIdentifierFromAttribute);
      }

      account.setNumber(item.get(ATTR_ACCOUNT_E164).s(), phoneNumberIdentifierFromAttribute);
      account.setUuid(accountIdentifier);
      account.setUsernameHash(AttributeValues.getByteArray(item, ATTR_USERNAME_HASH, null));
      account.setUsernameLinkHandle(AttributeValues.getUUID(item, ATTR_USERNAME_LINK_UUID, null));
      account.setVersion(Integer.parseInt(item.get(ATTR_VERSION).n()));

      return account;

    } catch (final IOException e) {
      throw new RuntimeException("Could not read stored account data", e);
    }
  }

  private static AttributeValue accountDataAttributeValue(final Account account) {
    try {
      return AttributeValues.fromByteArray(ACCOUNT_DDB_JSON_WRITER.writeValueAsBytes(account));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static boolean conditionalCheckFailed(final CancellationReason reason) {
    return CONDITIONAL_CHECK_FAILED.equals(reason.code());
  }

  private static boolean isTransactionConflict(final CancellationReason reason) {
    return TRANSACTION_CONFLICT.equals(reason.code());
  }

  private static String redactPhoneNumber(final String phoneNumber) {
    final StringBuilder sb = new StringBuilder();
    sb.append("+");
    sb.append(Util.getCountryCode(phoneNumber));
    sb.append("???");
    sb.append(StringUtils.length(phoneNumber) < 3
        ? ""
        : phoneNumber.substring(phoneNumber.length() - 2, phoneNumber.length()));
    return sb.toString();
  }
}
