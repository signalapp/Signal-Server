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
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.AsyncTimerUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
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
public class Accounts extends AbstractDynamoDbStore {

  private static final Logger log = LoggerFactory.getLogger(Accounts.class);

  private static final Duration USERNAME_RECLAIM_TTL = Duration.ofDays(3);

  static final List<String> ACCOUNT_FIELDS_TO_EXCLUDE_FROM_SERIALIZATION = List.of("uuid", "usernameLinkHandle");

  private static final ObjectWriter ACCOUNT_DDB_JSON_WRITER = SystemMapper.jsonMapper()
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
  // confirmed; bool
  static final String ATTR_CONFIRMED = "F";
  // reclaimable; bool. Indicates that on confirmation the username link should be preserved
  static final String ATTR_RECLAIMABLE = "R";
  // unidentified access key; byte[] or null
  static final String ATTR_UAK = "UAK";
  // time to live; number
  static final String ATTR_TTL = "TTL";

  static final String DELETED_ACCOUNTS_KEY_ACCOUNT_E164 = "P";
  static final String DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID = "U";
  static final String DELETED_ACCOUNTS_ATTR_EXPIRES = "E";
  static final String DELETED_ACCOUNTS_UUID_TO_E164_INDEX_NAME = "u_to_p";

  static final String USERNAME_LINK_TO_UUID_INDEX = "ul_to_u";

  static final Duration DELETED_ACCOUNTS_TIME_TO_LIVE = Duration.ofDays(30);

  private final Clock clock;

  private final DynamoDbAsyncClient asyncClient;

  private final String phoneNumberConstraintTableName;
  private final String phoneNumberIdentifierConstraintTableName;
  private final String usernamesConstraintTableName;
  private final String deletedAccountsTableName;
  private final String accountsTableName;

  @VisibleForTesting
  public Accounts(
      final Clock clock,
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final String deletedAccountsTableName) {

    super(client);
    this.clock = clock;
    this.asyncClient = asyncClient;
    this.phoneNumberConstraintTableName = phoneNumberConstraintTableName;
    this.phoneNumberIdentifierConstraintTableName = phoneNumberIdentifierConstraintTableName;
    this.accountsTableName = accountsTableName;
    this.usernamesConstraintTableName = usernamesConstraintTableName;
    this.deletedAccountsTableName = deletedAccountsTableName;
  }

  public Accounts(
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final String deletedAccountsTableName) {

    this(Clock.systemUTC(), client, asyncClient, accountsTableName,
        phoneNumberConstraintTableName, phoneNumberIdentifierConstraintTableName, usernamesConstraintTableName,
        deletedAccountsTableName);
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
      final TransactWriteItem deletedAccountDelete = buildRemoveDeletedAccount(account.getNumber());

      final Collection<TransactWriteItem> writeItems = new ArrayList<>(
          List.of(phoneNumberConstraintPut, phoneNumberIdentifierConstraintPut, accountPut, deletedAccountDelete));

      writeItems.addAll(additionalWriteItems);

      final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
          .transactItems(writeItems)
          .build();

      try {
        db().transactWriteItems(request);
      } catch (final TransactionCanceledException e) {

        final CancellationReason accountCancellationReason = e.cancellationReasons().get(2);

        if (conditionalCheckFailed(accountCancellationReason)) {
          throw new IllegalArgumentException("account identifier present with different phone number");
        }

        final CancellationReason phoneNumberConstraintCancellationReason = e.cancellationReasons().get(0);
        final CancellationReason phoneNumberIdentifierConstraintCancellationReason = e.cancellationReasons().get(1);

        if (conditionalCheckFailed(phoneNumberConstraintCancellationReason)
            || conditionalCheckFailed(phoneNumberIdentifierConstraintCancellationReason)) {

          // In theory, both reasons should trip in tandem and either should give us the information we need. Even so,
          // we'll be cautious here and make sure we're choosing a condition check that really failed.
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
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
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
        !existingAccount.getNumber().equals(accountToCreate.getNumber())) {

      throw new IllegalArgumentException("reclaimed accounts must match");
    }

    return AsyncTimerUtil.record(RECLAIM_TIMER, () -> {

      accountToCreate.setVersion(existingAccount.getVersion());

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
                    KEY_ACCOUNT_UUID, AttributeValues.fromUUID(accountToCreate.getUuid()),
                    ATTR_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
                    ATTR_TTL, AttributeValues.fromLong(expirationTime),
                    ATTR_CONFIRMED, AttributeValues.fromBool(false),
                    ATTR_RECLAIMABLE, AttributeValues.fromBool(true)))
                .conditionExpression("attribute_not_exists(#username_hash) OR (#ttl < :now) OR #uuid = :uuid")
                .expressionAttributeNames(Map.of(
                    "#username_hash", ATTR_USERNAME_HASH,
                    "#ttl", ATTR_TTL,
                    "#uuid", KEY_ACCOUNT_UUID))
                .expressionAttributeValues(Map.of(
                    ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                    ":uuid", AttributeValues.fromUUID(accountToCreate.getUuid())))
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build())
            .build());
      }
      writeItems.add(UpdateAccountSpec.forAccount(accountsTableName, accountToCreate).transactItem());
      writeItems.addAll(additionalWriteItems);

      return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writeItems).build())
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
        writeItems.add(buildRemoveDeletedAccount(number));
        maybeDisplacedAccountIdentifier.ifPresent(displacedAccountIdentifier ->
            writeItems.add(buildPutDeletedAccount(displacedAccountIdentifier, originalNumber)));

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
                        ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
                        ":pni", pniAttr,
                        ":version", AttributeValues.fromInt(account.getVersion()),
                        ":version_increment", AttributeValues.fromInt(1)))
                    .build())
                .build());

        writeItems.addAll(additionalWriteItems);

        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(writeItems)
            .build();

        db().transactWriteItems(request);

        account.setVersion(account.getVersion() + 1);
        succeeded = true;
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
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

    // if there is an existing old reservation it will be cleaned up via ttl
    final Optional<byte[]> maybeOriginalReservation = account.getReservedUsernameHash();
    account.setReservedUsernameHash(reservedUsernameHash);

    final long expirationTime = clock.instant().plus(ttl).getEpochSecond();

    // Use account UUID as a "reservation token" - by providing this, the client proves ownership of the hash
    final UUID uuid = account.getUuid();
    final byte[] accountJsonBytes;

    try {
      accountJsonBytes = ACCOUNT_DDB_JSON_WRITER.writeValueAsBytes(account);
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    final List<TransactWriteItem> writeItems = new ArrayList<>();

    writeItems.add(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(usernamesConstraintTableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                ATTR_USERNAME_HASH, AttributeValues.fromByteArray(reservedUsernameHash),
                ATTR_TTL, AttributeValues.fromLong(expirationTime),
                ATTR_CONFIRMED, AttributeValues.fromBool(false),
                ATTR_RECLAIMABLE, AttributeValues.fromBool(false)))
            .conditionExpression("attribute_not_exists(#username_hash) OR #ttl < :now OR (#aci = :aci AND #confirmed = :confirmed)")
            .expressionAttributeNames(Map.of("#username_hash", ATTR_USERNAME_HASH, "#ttl", ATTR_TTL, "#aci", KEY_ACCOUNT_UUID, "#confirmed", ATTR_CONFIRMED))
            .expressionAttributeValues(Map.of(
                ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                ":aci", AttributeValues.fromUUID(uuid),
                ":confirmed", AttributeValues.fromBool(false)))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build());

    writeItems.add(
        TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid)))
                .updateExpression("SET #data = :data ADD #version :version_increment")
                .conditionExpression("#version = :version")
                .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA, "#version", ATTR_VERSION))
                .expressionAttributeValues(Map.of(
                    ":data", AttributeValues.fromByteArray(accountJsonBytes),
                    ":version", AttributeValues.fromInt(account.getVersion()),
                    ":version_increment", AttributeValues.fromInt(1)))
                .build())
            .build());

    return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(writeItems)
            .build())
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException e) {
            // If the constraint table update failed the condition check, the username's taken and we should stop
            // trying. However, if the accounts table fails the conditional check or
            // either table was concurrently updated, it's an optimistic locking failure and we should try again.
            if (conditionalCheckFailed(e.cancellationReasons().get(0))) {
              throw ExceptionUtils.wrap(new UsernameHashNotAvailableException());
            } else if (conditionalCheckFailed(e.cancellationReasons().get(1)) ||
                e.cancellationReasons().stream().anyMatch(Accounts::isTransactionConflict)) {
              throw new ContestedOptimisticLockException();
            }
          }

          throw ExceptionUtils.wrap(throwable);
        })
        .whenComplete((response, throwable) -> {
          sample.stop(RESERVE_USERNAME_TIMER);

          if (throwable == null) {
            account.setVersion(account.getVersion() + 1);
          } else {
            account.setReservedUsernameHash(maybeOriginalReservation.orElse(null));
          }
        })
        .thenRun(() -> {});
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

    return pickLinkHandle(account, usernameHash)
        .thenCompose(linkHandle -> {
          final TransactWriteItemsRequest request;
          try {
            final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
            updatedAccount.setUsernameHash(usernameHash);
            updatedAccount.setReservedUsernameHash(null);
            updatedAccount.setUsernameLinkDetails(encryptedUsername == null ? null : linkHandle, encryptedUsername);

            request = buildConfirmUsernameHashRequest(updatedAccount, account.getUsernameHash());
          } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException(e);
          }

          return asyncClient.transactWriteItems(request).thenApply(ignored -> linkHandle);
        })
        .thenApply(linkHandle -> {
          account.setUsernameHash(usernameHash);
          account.setReservedUsernameHash(null);
          account.setUsernameLinkDetails(encryptedUsername == null ? null : linkHandle, encryptedUsername);

          account.setVersion(account.getVersion() + 1);
          return (Void) null;
        })
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException e) {
            // If the constraint table update failed the condition check, the username's taken and we should stop
            // trying. However, if the accounts table fails the conditional check or
            // either table was concurrently updated, it's an optimistic locking failure and we should try again.
            // NOTE: the fixed indices here must be kept in sync with the creation of the TransactWriteItems in
            // buildConfirmUsernameHashRequest!
            if (conditionalCheckFailed(e.cancellationReasons().get(0))) {
              throw ExceptionUtils.wrap(new UsernameHashNotAvailableException());
            } else if (conditionalCheckFailed(e.cancellationReasons().get(1)) ||
                e.cancellationReasons().stream().anyMatch(Accounts::isTransactionConflict)) {
              throw new ContestedOptimisticLockException();
            }
          }

          throw ExceptionUtils.wrap(throwable);
        })
        .whenComplete((ignored, throwable) -> sample.stop(SET_USERNAME_TIMER));
  }

  private CompletableFuture<UUID> pickLinkHandle(final Account account, final byte[] usernameHash) {
    if (account.getUsernameLinkHandle() == null) {
      // There's no old link handle, so we can just use a randomly generated link handle
      return CompletableFuture.completedFuture(UUID.randomUUID());
    }

    // Otherwise, there's an existing link handle. If this is the result of an account being re-registered, we should
    // preserve the link handle.
    return asyncClient.getItem(GetItemRequest.builder()
            .tableName(usernamesConstraintTableName)
            .key(Map.of(ATTR_USERNAME_HASH, AttributeValues.b(usernameHash)))
            .projectionExpression(ATTR_RECLAIMABLE).build())
        .thenApply(response -> {
          if (response.hasItem() && AttributeValues.getBool(response.item(), ATTR_RECLAIMABLE, false)) {
            // this username reservation indicates it's a username waiting to be "reclaimed"
            return account.getUsernameLinkHandle();
          }
          // There was no existing username reservation, or this was a standard "new" username. Either way, we should
          // generate a new link handle.
          return UUID.randomUUID();
        });
  }

  private TransactWriteItemsRequest buildConfirmUsernameHashRequest(final Account updatedAccount,
      final Optional<byte[]> maybeOriginalUsernameHash)
      throws JsonProcessingException {

    final List<TransactWriteItem> writeItems = new ArrayList<>();
    final byte[] usernameHash = updatedAccount.getUsernameHash()
        .orElseThrow(() -> new IllegalArgumentException("Account must have a username hash"));

    // NOTE: the order in which writeItems are added to the list is significant, and must be kept in sync with the catch block in confirmUsernameHash!

    // add the username hash to the constraint table, wiping out the ttl if we had already reserved the hash
    writeItems.add(TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(usernamesConstraintTableName)
            .item(Map.of(
                KEY_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid()),
                ATTR_USERNAME_HASH, AttributeValues.fromByteArray(usernameHash),
                ATTR_CONFIRMED, AttributeValues.fromBool(true)))
            // it's not in the constraint table OR it's expired OR it was reserved by us
            .conditionExpression("attribute_not_exists(#username_hash) OR #ttl < :now OR (#aci = :aci AND #confirmed = :confirmed)")
            .expressionAttributeNames(Map.of("#username_hash", ATTR_USERNAME_HASH, "#ttl", ATTR_TTL, "#aci", KEY_ACCOUNT_UUID, "#confirmed", ATTR_CONFIRMED))
            .expressionAttributeValues(Map.of(
                ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                ":aci", AttributeValues.fromUUID(updatedAccount.getUuid()),
                ":confirmed", AttributeValues.fromBool(false)))
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build())
        .build());

    final StringBuilder updateExpr = new StringBuilder("SET #data = :data, #username_hash = :username_hash");
    final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>(Map.of(
        ":data", accountDataAttributeValue(updatedAccount),
        ":username_hash", AttributeValues.fromByteArray(usernameHash),
        ":version", AttributeValues.fromInt(updatedAccount.getVersion()),
        ":version_increment", AttributeValues.fromInt(1)));
    if (updatedAccount.getUsernameLinkHandle() != null) {
      updateExpr.append(", #ul = :ul");
      expressionAttributeValues.put(":ul", AttributeValues.fromUUID(updatedAccount.getUsernameLinkHandle()));
    } else {
      updateExpr.append(" REMOVE #ul");
    }
    updateExpr.append(" ADD #version :version_increment");

    writeItems.add(
        TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid())))
                .updateExpression(updateExpr.toString())
                .conditionExpression("#version = :version")
                .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA,
                    "#username_hash", ATTR_USERNAME_HASH,
                    "#ul", ATTR_USERNAME_LINK_UUID,
                    "#version", ATTR_VERSION))
                .expressionAttributeValues(expressionAttributeValues)
                .build())
            .build());

    maybeOriginalUsernameHash.ifPresent(originalUsernameHash -> writeItems.add(
        buildDelete(usernamesConstraintTableName, ATTR_USERNAME_HASH, originalUsernameHash)));

    return TransactWriteItemsRequest.builder()
        .transactItems(writeItems)
        .build();
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
    return account.getUsernameHash().map(usernameHash -> {
      final Timer.Sample sample = Timer.start();

      final TransactWriteItemsRequest request;

      try {
        final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
        updatedAccount.setUsernameHash(null);
        updatedAccount.setUsernameLinkDetails(null, null);

        request = buildClearUsernameHashRequest(updatedAccount, usernameHash);
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      return asyncClient.transactWriteItems(request)
          .thenAccept(ignored -> {
            account.setUsernameHash(null);
            account.setUsernameLinkDetails(null, null);

            account.setVersion(account.getVersion() + 1);
          })
          .exceptionally(throwable -> {
            if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException e) {
              if (conditionalCheckFailed(e.cancellationReasons().get(0)) ||
                  e.cancellationReasons().stream().anyMatch(Accounts::isTransactionConflict)) {
                throw new ContestedOptimisticLockException();
              }
            }

            throw ExceptionUtils.wrap(throwable);
          })
          .whenComplete((ignored, throwable) -> sample.stop(CLEAR_USERNAME_HASH_TIMER));
    }).orElseGet(() -> CompletableFuture.completedFuture(null));
  }

  private TransactWriteItemsRequest buildClearUsernameHashRequest(final Account updatedAccount, final byte[] originalUsernameHash)
      throws JsonProcessingException {

    final List<TransactWriteItem> writeItems = new ArrayList<>();

    writeItems.add(
        TransactWriteItem.builder()
            .update(Update.builder()
                .tableName(accountsTableName)
                .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(updatedAccount.getUuid())))
                .updateExpression("SET #data = :data REMOVE #username_hash, #username_link ADD #version :version_increment")
                .conditionExpression("#version = :version")
                .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA,
                    "#username_hash", ATTR_USERNAME_HASH,
                    "#username_link", ATTR_USERNAME_LINK_UUID,
                    "#version", ATTR_VERSION))
                .expressionAttributeValues(Map.of(
                    ":data", accountDataAttributeValue(updatedAccount),
                    ":version", AttributeValues.fromInt(updatedAccount.getVersion()),
                    ":version_increment", AttributeValues.fromInt(1)))
                .build())
            .build());

    writeItems.add(buildDelete(usernamesConstraintTableName, ATTR_USERNAME_HASH, originalUsernameHash));

    return TransactWriteItemsRequest.builder()
        .transactItems(writeItems)
        .build();
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
      try {
        // username, e164, and pni cannot be modified through this method
        final Map<String, String> attrNames = new HashMap<>(Map.of(
            "#number", ATTR_ACCOUNT_E164,
            "#data", ATTR_ACCOUNT_DATA,
            "#cds", ATTR_CANONICALLY_DISCOVERABLE,
            "#version", ATTR_VERSION));

        final Map<String, AttributeValue> attrValues = new HashMap<>(Map.of(
            ":data", accountDataAttributeValue(account),
            ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
            ":version", AttributeValues.fromInt(account.getVersion()),
            ":version_increment", AttributeValues.fromInt(1)));

        final StringBuilder updateExpressionBuilder = new StringBuilder("SET #data = :data, #cds = :cds");
        if (account.getUnidentifiedAccessKey().isPresent()) {
          // if it's present in the account, also set the uak
          attrNames.put("#uak", ATTR_UAK);
          attrValues.put(":uak", AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
          updateExpressionBuilder.append(", #uak = :uak");
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
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @Nonnull
  public CompletionStage<Void> updateAsync(final Account account) {
    return AsyncTimerUtil.record(UPDATE_TIMER, () -> {
      final UpdateItemRequest updateItemRequest = UpdateAccountSpec
          .forAccount(accountsTableName, account)
          .updateItemRequest();

      return asyncClient.updateItem(updateItemRequest)
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

      return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
              .transactItems(writeItems)
              .build())
          .thenApply(response -> {
            account.setVersion(account.getVersion() + 1);
            return (Void) null;
          })
          .exceptionally(throwable -> {
            final Throwable unwrapped = ExceptionUtils.unwrap(throwable);

            if (unwrapped instanceof TransactionCanceledException transactionCanceledException) {
              if (CONDITIONAL_CHECK_FAILED.equals(transactionCanceledException.cancellationReasons().get(0).code())) {
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
        ATTR_USERNAME_HASH,
        AttributeValues.fromByteArray(usernameHash),
        item -> AttributeValues.getBool(item, ATTR_CONFIRMED, false) // ignore items that are reservations (not confirmed)
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

  private TransactWriteItem buildPutDeletedAccount(final UUID uuid, final String e164) {
    return TransactWriteItem.builder()
        .put(Put.builder()
            .tableName(deletedAccountsTableName)
            .item(Map.of(
                DELETED_ACCOUNTS_KEY_ACCOUNT_E164, AttributeValues.fromString(e164),
                DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID, AttributeValues.fromUUID(uuid),
                DELETED_ACCOUNTS_ATTR_EXPIRES, AttributeValues.fromLong(Instant.now().plus(DELETED_ACCOUNTS_TIME_TO_LIVE).getEpochSecond())))
            .build())
        .build();
  }

  private TransactWriteItem buildRemoveDeletedAccount(final String e164) {
    return TransactWriteItem.builder()
        .delete(Delete.builder()
            .tableName(deletedAccountsTableName)
            .key(Map.of(DELETED_ACCOUNTS_KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
            .build())
        .build();
  }

  @Nonnull
  public CompletableFuture<Optional<Account>> getByAccountIdentifierAsync(final UUID uuid) {
    return AsyncTimerUtil.record(GET_BY_UUID_TIMER, () -> itemByKeyAsync(accountsTableName, KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))
        .thenApply(maybeItem -> maybeItem.map(Accounts::fromItem)))
        .toCompletableFuture();
  }

  public Optional<UUID> findRecentlyDeletedAccountIdentifier(final String e164) {
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(deletedAccountsTableName)
        .consistentRead(true)
        .key(Map.of(DELETED_ACCOUNTS_KEY_ACCOUNT_E164, AttributeValues.fromString(e164)))
        .build());

    return Optional.ofNullable(AttributeValues.getUUID(response.item(), DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID, null));
  }

  public Optional<String> findRecentlyDeletedE164(final UUID uuid) {
    final QueryResponse response = db().query(QueryRequest.builder()
        .tableName(deletedAccountsTableName)
        .indexName(DELETED_ACCOUNTS_UUID_TO_E164_INDEX_NAME)
        .keyConditionExpression("#uuid = :uuid")
        .projectionExpression("#e164")
        .expressionAttributeNames(Map.of("#uuid", DELETED_ACCOUNTS_ATTR_ACCOUNT_UUID,
            "#e164", DELETED_ACCOUNTS_KEY_ACCOUNT_E164))
        .expressionAttributeValues(Map.of(":uuid", AttributeValues.fromUUID(uuid))).build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new RuntimeException("Impossible result: more than one phone number returned for UUID: " + uuid);
    }

    return Optional.ofNullable(response.items().get(0).get(DELETED_ACCOUNTS_KEY_ACCOUNT_E164).s());
  }

  public CompletableFuture<Void> delete(final UUID uuid, final List<TransactWriteItem> additionalWriteItems) {
    final Timer.Sample sample = Timer.start();

    return getByAccountIdentifierAsync(uuid)
        .thenCompose(maybeAccount -> maybeAccount.map(account -> {
              final List<TransactWriteItem> transactWriteItems = new ArrayList<>(List.of(
                  buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, account.getNumber()),
                  buildDelete(accountsTableName, KEY_ACCOUNT_UUID, uuid),
                  buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, account.getPhoneNumberIdentifier()),
                  buildPutDeletedAccount(uuid, account.getNumber())
              ));

              account.getUsernameHash().ifPresent(usernameHash -> transactWriteItems.add(
                  buildDelete(usernamesConstraintTableName, ATTR_USERNAME_HASH, usernameHash)));

              transactWriteItems.addAll(additionalWriteItems);

              return asyncClient.transactWriteItems(TransactWriteItemsRequest.builder()
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
        .flatMap(segment -> asyncClient.scanPaginator(ScanRequest.builder()
                .tableName(accountsTableName)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .build())
            .items()
            .map(Accounts::fromItem))
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
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(table)
        .key(Map.of(keyName, keyValue))
        .consistentRead(true)
        .build());
    return Optional.ofNullable(response.item()).filter(m -> !m.isEmpty());
  }

  @Nonnull
  private CompletableFuture<Optional<Map<String, AttributeValue>>> itemByKeyAsync(final String table, final String keyName, final AttributeValue keyValue) {
    return asyncClient.getItem(GetItemRequest.builder()
            .tableName(table)
            .key(Map.of(keyName, keyValue))
            .consistentRead(true)
            .build())
        .thenApply(response -> Optional.ofNullable(response.item()).filter(item -> !item.isEmpty()));
  }

  @Nonnull
  private Optional<Map<String, AttributeValue>> itemByGsiKey(final String table, final String indexName, final String keyName, final AttributeValue keyValue) {
    final QueryResponse response = db().query(QueryRequest.builder()
        .tableName(table)
        .indexName(indexName)
        .keyConditionExpression("#gsiKey = :gsiValue")
        .projectionExpression("#uuid")
        .expressionAttributeNames(Map.of(
            "#gsiKey", keyName,
            "#uuid", KEY_ACCOUNT_UUID))
        .expressionAttributeValues(Map.of(
            ":gsiValue", keyValue))
        .build());

    if (response.count() == 0) {
      return Optional.empty();
    }

    if (response.count() > 1) {
      throw new IllegalStateException("More than one row located for GSI [%s], key-value pair [%s, %s]"
          .formatted(indexName, keyName, keyValue));
    }

    final AttributeValue primaryKeyValue = response.items().get(0).get(KEY_ACCOUNT_UUID);
    return itemByKey(table, KEY_ACCOUNT_UUID, primaryKeyValue);
  }

  @Nonnull
  private CompletableFuture<Optional<Map<String, AttributeValue>>> itemByGsiKeyAsync(final String table, final String indexName, final String keyName, final AttributeValue keyValue) {
    return asyncClient.query(QueryRequest.builder()
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

          final AttributeValue primaryKeyValue = response.items().get(0).get(KEY_ACCOUNT_UUID);
          return itemByKeyAsync(table, KEY_ACCOUNT_UUID, primaryKeyValue);
        });
  }

  @Nonnull
  private TransactWriteItem buildAccountPut(
      final Account account,
      final AttributeValue uuidAttr,
      final AttributeValue numberAttr,
      final AttributeValue pniUuidAttr) throws JsonProcessingException {

    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        KEY_ACCOUNT_UUID, uuidAttr,
        ATTR_ACCOUNT_E164, numberAttr,
        ATTR_PNI_UUID, pniUuidAttr,
        ATTR_ACCOUNT_DATA, accountDataAttributeValue(account),
        ATTR_VERSION, AttributeValues.fromInt(account.getVersion()),
        ATTR_CANONICALLY_DISCOVERABLE, AttributeValues.fromBool(account.shouldBeVisibleInDirectory())));

    // Add the UAK if it's in the account
    account.getUnidentifiedAccessKey()
        .map(AttributeValues::fromByteArray)
        .ifPresent(uak -> item.put(ATTR_UAK, uak));

    return TransactWriteItem.builder()
        .put(Put.builder()
            .conditionExpression("attribute_not_exists(#number) OR #number = :number")
            .expressionAttributeNames(Map.of("#number", ATTR_ACCOUNT_E164))
            .expressionAttributeValues(Map.of(":number", numberAttr))
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
      final AttributeValue keyValue
  ) {
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

  private static AttributeValue accountDataAttributeValue(final Account account) throws JsonProcessingException {
    return AttributeValues.fromByteArray(ACCOUNT_DDB_JSON_WRITER.writeValueAsBytes(account));
  }

  private static boolean conditionalCheckFailed(final CancellationReason reason) {
    return CONDITIONAL_CHECK_FAILED.equals(reason.code());
  }

  private static boolean isTransactionConflict(final CancellationReason reason) {
    return TRANSACTION_CONFLICT.equals(reason.code());
  }
}
