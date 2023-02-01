/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.UsernameNormalizer;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.CompletableFutureUtils;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class Accounts extends AbstractDynamoDbStore {

  private static final Logger log = LoggerFactory.getLogger(Accounts.class);

  private static final byte RESERVED_USERNAME_HASH_VERSION = 1;

  private static final Timer CREATE_TIMER = Metrics.timer(name(Accounts.class, "create"));
  private static final Timer CHANGE_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "changeNumber"));
  private static final Timer SET_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "setUsername"));
  private static final Timer RESERVE_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "reserveUsername"));
  private static final Timer CLEAR_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "clearUsername"));
  private static final Timer UPDATE_TIMER = Metrics.timer(name(Accounts.class, "update"));
  private static final Timer GET_BY_NUMBER_TIMER = Metrics.timer(name(Accounts.class, "getByNumber"));
  private static final Timer GET_BY_USERNAME_TIMER = Metrics.timer(name(Accounts.class, "getByUsername"));
  private static final Timer GET_BY_PNI_TIMER = Metrics.timer(name(Accounts.class, "getByPni"));
  private static final Timer GET_BY_UUID_TIMER = Metrics.timer(name(Accounts.class, "getByUuid"));
  private static final Timer GET_ALL_FROM_START_TIMER = Metrics.timer(name(Accounts.class, "getAllFrom"));
  private static final Timer GET_ALL_FROM_OFFSET_TIMER = Metrics.timer(name(Accounts.class, "getAllFromOffset"));
  private static final Timer DELETE_TIMER = Metrics.timer(name(Accounts.class, "delete"));

  private static final String CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailed";

  private static final String TRANSACTION_CONFLICT = "TransactionConflict";

  // uuid, primary key
  static final String KEY_ACCOUNT_UUID = "U";
  // uuid, attribute on account table, primary key for PNI table
  static final String ATTR_PNI_UUID = "PNI";
  // phone number
  static final String ATTR_ACCOUNT_E164 = "P";
  // account, serialized to JSON
  static final String ATTR_ACCOUNT_DATA = "D";
  // internal version for optimistic locking
  static final String ATTR_VERSION = "V";
  // canonically discoverable
  static final String ATTR_CANONICALLY_DISCOVERABLE = "C";
  // username; string
  static final String ATTR_USERNAME = "N";
  // unidentified access key; byte[] or null
  static final String ATTR_UAK = "UAK";
  // time to live; number
  static final String ATTR_TTL = "TTL";

  private final Clock clock;

  private final DynamoDbAsyncClient asyncClient;

  private final String phoneNumberConstraintTableName;

  private final String phoneNumberIdentifierConstraintTableName;

  private final String usernamesConstraintTableName;

  private final String accountsTableName;

  private final int scanPageSize;


  @VisibleForTesting
  public Accounts(
      final Clock clock,
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final int scanPageSize) {
    super(client);
    this.clock = clock;
    this.asyncClient = asyncClient;
    this.phoneNumberConstraintTableName = phoneNumberConstraintTableName;
    this.phoneNumberIdentifierConstraintTableName = phoneNumberIdentifierConstraintTableName;
    this.accountsTableName = accountsTableName;
    this.usernamesConstraintTableName = usernamesConstraintTableName;
    this.scanPageSize = scanPageSize;
  }

  public Accounts(
      final DynamoDbClient client,
      final DynamoDbAsyncClient asyncClient,
      final String accountsTableName,
      final String phoneNumberConstraintTableName,
      final String phoneNumberIdentifierConstraintTableName,
      final String usernamesConstraintTableName,
      final int scanPageSize) {
    this(Clock.systemUTC(), client, asyncClient, accountsTableName,
        phoneNumberConstraintTableName, phoneNumberIdentifierConstraintTableName, usernamesConstraintTableName,
        scanPageSize);
  }

  public boolean create(final Account account) {
    return CREATE_TIMER.record(() -> {
      try {
        final AttributeValue uuidAttr = AttributeValues.fromUUID(account.getUuid());
        final AttributeValue numberAttr = AttributeValues.fromString(account.getNumber());
        final AttributeValue pniUuidAttr = AttributeValues.fromUUID(account.getPhoneNumberIdentifier());

        final TransactWriteItem phoneNumberConstraintPut = buildConstraintTablePutIfAbsent(
            phoneNumberConstraintTableName, uuidAttr, ATTR_ACCOUNT_E164, numberAttr);

        final TransactWriteItem phoneNumberIdentifierConstraintPut = buildConstraintTablePutIfAbsent(
            phoneNumberIdentifierConstraintTableName, uuidAttr, ATTR_PNI_UUID, pniUuidAttr);

        final TransactWriteItem accountPut = buildAccountPut(account, uuidAttr, numberAttr, pniUuidAttr);

        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(phoneNumberConstraintPut, phoneNumberIdentifierConstraintPut, accountPut)
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

            final ByteBuffer actualAccountUuid = reason.item().get(KEY_ACCOUNT_UUID).b().asByteBuffer();
            account.setUuid(UUIDUtil.fromByteBuffer(actualAccountUuid));

            final Account existingAccount = getByAccountIdentifier(account.getUuid()).orElseThrow();
            account.setNumber(existingAccount.getNumber(), existingAccount.getPhoneNumberIdentifier());
            account.setVersion(existingAccount.getVersion());

            update(account);

            return false;
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
      }

      return true;
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
  public void changeNumber(final Account account, final String number, final UUID phoneNumberIdentifier) {
    CHANGE_NUMBER_TIMER.record(() -> {
      final String originalNumber = account.getNumber();
      final UUID originalPni = account.getPhoneNumberIdentifier();

      boolean succeeded = false;

      account.setNumber(number, phoneNumberIdentifier);

      try {
        final List<TransactWriteItem> writeItems = new ArrayList<>();
        final AttributeValue uuidAttr = AttributeValues.fromUUID(account.getUuid());
        final AttributeValue numberAttr = AttributeValues.fromString(number);
        final AttributeValue pniAttr = AttributeValues.fromUUID(phoneNumberIdentifier);

        writeItems.add(buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, originalNumber));
        writeItems.add(buildConstraintTablePut(phoneNumberConstraintTableName, uuidAttr, ATTR_ACCOUNT_E164, numberAttr));
        writeItems.add(buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, originalPni));
        writeItems.add(buildConstraintTablePut(phoneNumberIdentifierConstraintTableName, uuidAttr, ATTR_PNI_UUID, pniAttr));
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
                        ":data", AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
                        ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
                        ":pni", pniAttr,
                        ":version", AttributeValues.fromInt(account.getVersion()),
                        ":version_increment", AttributeValues.fromInt(1)))
                    .build())
                .build());

        final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(writeItems)
            .build();

        db().transactWriteItems(request);

        account.setVersion(account.getVersion() + 1);
        succeeded = true;
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      } finally {
        if (!succeeded) {
          account.setNumber(originalNumber, originalPni);
        }
      }
    });
  }

  /**
   * Reserve a username under a token
   *
   * @return a reservation token that must be provided when {@link #confirmUsername(Account, String, UUID)} is called
   */
  public UUID reserveUsername(
      final Account account,
      final String reservedUsername,
      final Duration ttl) {
    final long startNanos = System.nanoTime();
    // if there is an existing old reservation it will be cleaned up via ttl
    final Optional<byte[]> maybeOriginalReservation = account.getReservedUsernameHash();
    account.setReservedUsernameHash(reservedUsernameHash(account.getUuid(), reservedUsername));

    boolean succeeded = false;

    final long expirationTime = clock.instant().plus(ttl).getEpochSecond();

    final UUID reservationToken = UUID.randomUUID();
    try {
      final List<TransactWriteItem> writeItems = new ArrayList<>();

      writeItems.add(TransactWriteItem.builder()
          .put(Put.builder()
              .tableName(usernamesConstraintTableName)
              .item(Map.of(
                  KEY_ACCOUNT_UUID, AttributeValues.fromUUID(reservationToken),
                  ATTR_USERNAME, AttributeValues.fromString(UsernameNormalizer.normalize(reservedUsername)),
                  ATTR_TTL, AttributeValues.fromLong(expirationTime)))
              .conditionExpression("attribute_not_exists(#username) OR (#ttl < :now)")
              .expressionAttributeNames(Map.of("#username", ATTR_USERNAME, "#ttl", ATTR_TTL))
              .expressionAttributeValues(Map.of(":now", AttributeValues.fromLong(clock.instant().getEpochSecond())))
              .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
              .build())
          .build());

      writeItems.add(
          TransactWriteItem.builder()
              .update(Update.builder()
                  .tableName(accountsTableName)
                  .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
                  .updateExpression("SET #data = :data ADD #version :version_increment")
                  .conditionExpression("#version = :version")
                  .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA, "#version", ATTR_VERSION))
                  .expressionAttributeValues(Map.of(
                      ":data", AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
                      ":version", AttributeValues.fromInt(account.getVersion()),
                      ":version_increment", AttributeValues.fromInt(1)))
                  .build())
              .build());

      final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
          .transactItems(writeItems)
          .build();

      db().transactWriteItems(request);

      account.setVersion(account.getVersion() + 1);
      succeeded = true;
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    } catch (final TransactionCanceledException e) {
      if (e.cancellationReasons().stream().map(CancellationReason::code).anyMatch(CONDITIONAL_CHECK_FAILED::equals)) {
        throw new ContestedOptimisticLockException();
      }
      throw e;
    } finally {
      if (!succeeded) {
        account.setReservedUsernameHash(maybeOriginalReservation.orElse(null));
      }
      RESERVE_USERNAME_TIMER.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }
    return reservationToken;
  }

  /**
   * Confirm (set) a previously reserved username
   *
   * @param account to update
   * @param username believed to be available
   * @param reservationToken a token returned by the call to {@link #reserveUsername(Account, String, Duration)},
   *                         only required if setting a reserved username
   * @throws ContestedOptimisticLockException if the account has been updated or the username taken by someone else
   */
  public void confirmUsername(final Account account, final String username, final UUID reservationToken)
      throws ContestedOptimisticLockException {
    setUsername(account, username, Optional.of(reservationToken));
  }

  /**
   * Set the account username
   *
   * @param account to update
   * @param username believed to be available
   * @throws ContestedOptimisticLockException if the account has been updated or the username taken by someone else
   */
  public void setUsername(final Account account, final String username) throws ContestedOptimisticLockException {
    setUsername(account, username, Optional.empty());
  }

  private void setUsername(final Account account, final String username, final Optional<UUID> reservationToken)
      throws ContestedOptimisticLockException {
    final long startNanos = System.nanoTime();

    final Optional<String> maybeOriginalUsername = account.getUsername();
    final Optional<byte[]> maybeOriginalReservation = account.getReservedUsernameHash();

    account.setUsername(username);
    account.setReservedUsernameHash(null);

    boolean succeeded = false;

    try {
      final List<TransactWriteItem> writeItems = new ArrayList<>();

      // add the username to the constraint table, wiping out the ttl if we had already reserved the name
      // Persist the normalized username in the usernamesConstraint table and the original username in the accounts table
      writeItems.add(TransactWriteItem.builder()
          .put(Put.builder()
              .tableName(usernamesConstraintTableName)
              .item(Map.of(
                  KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid()),
                  ATTR_USERNAME, AttributeValues.fromString(UsernameNormalizer.normalize(username))))
              // it's not in the constraint table OR it's expired OR it was reserved by us
              .conditionExpression("attribute_not_exists(#username) OR #ttl < :now OR #aci = :reservation ")
              .expressionAttributeNames(Map.of("#username", ATTR_USERNAME, "#ttl", ATTR_TTL, "#aci", KEY_ACCOUNT_UUID))
              .expressionAttributeValues(Map.of(
                  ":now", AttributeValues.fromLong(clock.instant().getEpochSecond()),
                  ":reservation", AttributeValues.fromUUID(reservationToken.orElseGet(UUID::randomUUID))))
              .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
              .build())
          .build());

      writeItems.add(
          TransactWriteItem.builder()
              .update(Update.builder()
                  .tableName(accountsTableName)
                  .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
                  .updateExpression("SET #data = :data, #username = :username ADD #version :version_increment")
                  .conditionExpression("#version = :version")
                  .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA,
                      "#username", ATTR_USERNAME,
                      "#version", ATTR_VERSION))
                  .expressionAttributeValues(Map.of(
                      ":data", AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
                      ":username", AttributeValues.fromString(username),
                      ":version", AttributeValues.fromInt(account.getVersion()),
                      ":version_increment", AttributeValues.fromInt(1)))
                  .build())
              .build());

      maybeOriginalUsername.ifPresent(originalUsername -> writeItems.add(
          buildDelete(usernamesConstraintTableName, ATTR_USERNAME, UsernameNormalizer.normalize(originalUsername))));

      final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
          .transactItems(writeItems)
          .build();

      db().transactWriteItems(request);

      account.setVersion(account.getVersion() + 1);
      succeeded = true;
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    } catch (final TransactionCanceledException e) {
      if (e.cancellationReasons().stream().map(CancellationReason::code).anyMatch(CONDITIONAL_CHECK_FAILED::equals)) {
        throw new ContestedOptimisticLockException();
      }
      throw e;
    } finally {
      if (!succeeded) {
        account.setUsername(maybeOriginalUsername.orElse(null));
        account.setReservedUsernameHash(maybeOriginalReservation.orElse(null));
      }
      SET_USERNAME_TIMER.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }
  }

  public void clearUsername(final Account account) {
    account.getUsername().ifPresent(username -> {
      CLEAR_USERNAME_TIMER.record(() -> {
        account.setUsername(null);

        boolean succeeded = false;

        try {
          final List<TransactWriteItem> writeItems = new ArrayList<>();

          writeItems.add(
              TransactWriteItem.builder()
                  .update(Update.builder()
                      .tableName(accountsTableName)
                      .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
                      .updateExpression("SET #data = :data REMOVE #username ADD #version :version_increment")
                      .conditionExpression("#version = :version")
                      .expressionAttributeNames(Map.of("#data", ATTR_ACCOUNT_DATA,
                          "#username", ATTR_USERNAME,
                          "#version", ATTR_VERSION))
                      .expressionAttributeValues(Map.of(
                          ":data", AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
                          ":version", AttributeValues.fromInt(account.getVersion()),
                          ":version_increment", AttributeValues.fromInt(1)))
                      .build())
                  .build());

          writeItems.add(buildDelete(usernamesConstraintTableName, ATTR_USERNAME, UsernameNormalizer.normalize(username)));

          final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
              .transactItems(writeItems)
              .build();

          db().transactWriteItems(request);

          account.setVersion(account.getVersion() + 1);
          succeeded = true;
        } catch (final JsonProcessingException e) {
          throw new IllegalArgumentException(e);
        } catch (final TransactionCanceledException e) {
          if (conditionalCheckFailed(e.cancellationReasons().get(0))) {
            throw new ContestedOptimisticLockException();
          }

          throw e;
        } finally {
          if (!succeeded) {
            account.setUsername(username);
          }
        }
      });
    });
  }

  @Nonnull
  public CompletionStage<Void> updateAsync(final Account account) {
    return record(UPDATE_TIMER, () -> {
      final UpdateItemRequest updateItemRequest;
      try {
        // username, e164, and pni cannot be modified through this method
        final Map<String, String> attrNames = new HashMap<>(Map.of(
            "#number", ATTR_ACCOUNT_E164,
            "#data", ATTR_ACCOUNT_DATA,
            "#cds", ATTR_CANONICALLY_DISCOVERABLE,
            "#version", ATTR_VERSION));
        final Map<String, AttributeValue> attrValues = new HashMap<>(Map.of(
            ":data", AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
            ":cds", AttributeValues.fromBool(account.shouldBeVisibleInDirectory()),
            ":version", AttributeValues.fromInt(account.getVersion()),
            ":version_increment", AttributeValues.fromInt(1)));

        final String updateExpression;
        if (account.getUnidentifiedAccessKey().isPresent()) {
          // if it's present in the account, also set the uak
          attrNames.put("#uak", ATTR_UAK);
          attrValues.put(":uak", AttributeValues.fromByteArray(account.getUnidentifiedAccessKey().get()));
          updateExpression = "SET #data = :data, #cds = :cds, #uak = :uak ADD #version :version_increment";
        } else {
          updateExpression = "SET #data = :data, #cds = :cds ADD #version :version_increment";
        }

        updateItemRequest = UpdateItemRequest.builder()
            .tableName(accountsTableName)
            .key(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(account.getUuid())))
            .updateExpression(updateExpression)
            .conditionExpression("attribute_exists(#number) AND #version = :version")
            .expressionAttributeNames(attrNames)
            .expressionAttributeValues(attrValues)
            .build();
      } catch (final JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }

      return asyncClient.updateItem(updateItemRequest)
          .thenApply(response -> {
            account.setVersion(AttributeValues.getInt(response.attributes(), "V", account.getVersion() + 1));
            return (Void) null;
          })
          .exceptionally(throwable -> {
            final Throwable unwrapped = ExceptionUtils.unwrap(throwable);
            if (unwrapped instanceof TransactionConflictException) {
              throw new ContestedOptimisticLockException();
            } else if (unwrapped instanceof ConditionalCheckFailedException e) {
              // the exception doesn't give details about which condition failed,
              // but we can infer it was an optimistic locking failure if the UUID is known
              throw getByAccountIdentifier(account.getUuid()).isPresent() ? new ContestedOptimisticLockException() : e;
            } else {
              // rethrow
              throw CompletableFutureUtils.errorAsCompletionException(throwable);
            }
          });
    });
  }

  public void update(final Account account) throws ContestedOptimisticLockException {
    try {
      updateAsync(account).toCompletableFuture().join();
    } catch (final CompletionException e) {
      // unwrap CompletionExceptions, throw as long is it's unchecked
      Throwables.throwIfUnchecked(ExceptionUtils.unwrap(e));

      // if we otherwise somehow got a wrapped checked exception,
      // rethrow the checked exception wrapped by the original CompletionException
      log.error("Unexpected checked exception thrown from dynamo update", e);
      throw e;
    }
  }

  public boolean usernameAvailable(final String username) {
    return usernameAvailable(Optional.empty(), username);
  }

  public boolean usernameAvailable(final Optional<UUID> reservationToken, final String username) {
    final Optional<Map<String, AttributeValue>> usernameItem = itemByKey(
        usernamesConstraintTableName, ATTR_USERNAME, AttributeValues.fromString(UsernameNormalizer.normalize(username)));

    if (usernameItem.isEmpty()) {
      // username is free
      return true;
    }
    final Map<String, AttributeValue> item = usernameItem.get();

    if (AttributeValues.getLong(item, ATTR_TTL, Long.MAX_VALUE) < clock.instant().getEpochSecond()) {
      // username was reserved, but has expired
      return true;
    }

    // username is reserved by us
    return reservationToken
        .map(AttributeValues.getUUID(item, KEY_ACCOUNT_UUID, new UUID(0, 0))::equals)
        .orElse(false);
  }

  @Nonnull
  public Optional<Account> getByE164(final String number) {
    return getByIndirectLookup(
        GET_BY_NUMBER_TIMER, phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, AttributeValues.fromString(number));
  }

  @Nonnull
  public Optional<Account> getByPhoneNumberIdentifier(final UUID phoneNumberIdentifier) {
    return getByIndirectLookup(
        GET_BY_PNI_TIMER, phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, AttributeValues.fromUUID(phoneNumberIdentifier));
  }

  @Nonnull
  public Optional<Account> getByUsername(final String username) {
    return getByIndirectLookup(
        GET_BY_USERNAME_TIMER,
        usernamesConstraintTableName,
        ATTR_USERNAME,
        AttributeValues.fromString(UsernameNormalizer.normalize(username)),
        item -> !item.containsKey(ATTR_TTL) // ignore items with a ttl (reservations)
    );
  }

  @Nonnull
  public Optional<Account> getByAccountIdentifier(final UUID uuid) {
    return requireNonNull(GET_BY_UUID_TIMER.record(() ->
            itemByKey(accountsTableName, KEY_ACCOUNT_UUID, AttributeValues.fromUUID(uuid))
                .map(Accounts::fromItem)));
  }

  public void delete(final UUID uuid) {
    DELETE_TIMER.record(() -> getByAccountIdentifier(uuid).ifPresent(account -> {

      final List<TransactWriteItem> transactWriteItems = new ArrayList<>(List.of(
          buildDelete(phoneNumberConstraintTableName, ATTR_ACCOUNT_E164, account.getNumber()),
          buildDelete(accountsTableName, KEY_ACCOUNT_UUID, uuid),
          buildDelete(phoneNumberIdentifierConstraintTableName, ATTR_PNI_UUID, account.getPhoneNumberIdentifier())
      ));

      account.getUsername().ifPresent(username -> transactWriteItems.add(
          buildDelete(usernamesConstraintTableName, ATTR_USERNAME, UsernameNormalizer.normalize(username))));

      final TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
          .transactItems(transactWriteItems).build();
      db().transactWriteItems(request);
    }));
  }

  @Nonnull
  public AccountCrawlChunk getAllFrom(final UUID from, final int maxCount) {
    final ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
        .limit(scanPageSize)
        .exclusiveStartKey(Map.of(KEY_ACCOUNT_UUID, AttributeValues.fromUUID(from)));

    return scanForChunk(scanRequestBuilder, maxCount, GET_ALL_FROM_OFFSET_TIMER);
  }

  @Nonnull
  public AccountCrawlChunk getAllFromStart(final int maxCount) {
    final ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
        .limit(scanPageSize);

    return scanForChunk(scanRequestBuilder, maxCount, GET_ALL_FROM_START_TIMER);
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
  private Optional<Map<String, AttributeValue>> itemByKey(final String table, final String keyName, final AttributeValue keyValue) {
    final GetItemResponse response = db().getItem(GetItemRequest.builder()
        .tableName(table)
        .key(Map.of(keyName, keyValue))
        .consistentRead(true)
        .build());
    return Optional.ofNullable(response.item()).filter(m -> !m.isEmpty());
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
        ATTR_ACCOUNT_DATA, AttributeValues.fromByteArray(SystemMapper.getMapper().writeValueAsBytes(account)),
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
  private static <T> CompletionStage<T> record(final Timer timer, final Supplier<CompletionStage<T>> toRecord)  {
    final Timer.Sample sample = Timer.start();
    return toRecord.get().whenComplete((ignoreT, ignoreE) -> sample.stop(timer));
  }

  @Nonnull
  private AccountCrawlChunk scanForChunk(final ScanRequest.Builder scanRequestBuilder, final int maxCount, final Timer timer) {
    scanRequestBuilder.tableName(accountsTableName);
    final List<Map<String, AttributeValue>> items = requireNonNull(timer.record(() -> scan(scanRequestBuilder.build(), maxCount)));
    final List<Account> accounts = items.stream().map(Accounts::fromItem).toList();
    return new AccountCrawlChunk(accounts, accounts.size() > 0 ? accounts.get(accounts.size() - 1).getUuid() : null);
  }

  @Nonnull
  private static String extractCancellationReasonCodes(final TransactionCanceledException exception) {
    return exception.cancellationReasons().stream()
        .map(CancellationReason::code)
        .collect(Collectors.joining(", "));
  }

  @Nonnull
  public static byte[] reservedUsernameHash(final UUID accountId, final String reservedUsername) {
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    final ByteBuffer byteBuffer = ByteBuffer.allocate(32 + 1);
    sha256.update(UsernameNormalizer.normalize(reservedUsername).getBytes(StandardCharsets.UTF_8));
    sha256.update(UUIDUtil.toBytes(accountId));
    byteBuffer.put(RESERVED_USERNAME_HASH_VERSION);
    byteBuffer.put(sha256.digest());
    return byteBuffer.array();
  }

  @VisibleForTesting
  @Nonnull
  static Account fromItem(final Map<String, AttributeValue> item) {
    // TODO: eventually require ATTR_CANONICALLY_DISCOVERABLE
    if (!item.containsKey(ATTR_ACCOUNT_DATA)
        || !item.containsKey(ATTR_ACCOUNT_E164)
        || !item.containsKey(KEY_ACCOUNT_UUID)) {
      throw new RuntimeException("item missing values");
    }
    try {
      final Account account = SystemMapper.getMapper().readValue(item.get(ATTR_ACCOUNT_DATA).b().asByteArray(), Account.class);

      final UUID accountIdentifier = UUIDUtil.fromByteBuffer(item.get(KEY_ACCOUNT_UUID).b().asByteBuffer());
      final UUID phoneNumberIdentifierFromAttribute = AttributeValues.getUUID(item, ATTR_PNI_UUID, null);

      if (account.getPhoneNumberIdentifier() == null || phoneNumberIdentifierFromAttribute == null ||
          !Objects.equals(account.getPhoneNumberIdentifier(), phoneNumberIdentifierFromAttribute)) {

        log.warn("Missing or mismatched PNIs for account {}. From JSON: {}; from attribute: {}",
            accountIdentifier, account.getPhoneNumberIdentifier(), phoneNumberIdentifierFromAttribute);
      }

      account.setNumber(item.get(ATTR_ACCOUNT_E164).s(), phoneNumberIdentifierFromAttribute);
      account.setUuid(accountIdentifier);
      account.setUsername(AttributeValues.getString(item, ATTR_USERNAME, null));
      account.setVersion(Integer.parseInt(item.get(ATTR_VERSION).n()));
      account.setCanonicallyDiscoverable(Optional.ofNullable(item.get(ATTR_CANONICALLY_DISCOVERABLE))
          .map(AttributeValue::bool)
          .orElse(false));

      return account;

    } catch (final IOException e) {
      throw new RuntimeException("Could not read stored account data", e);
    }
  }

  private static boolean conditionalCheckFailed(final CancellationReason reason) {
    return CONDITIONAL_CHECK_FAILED.equals(reason.code());
  }
}
