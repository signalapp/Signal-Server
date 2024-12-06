/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;
import static org.whispersystems.textsecuregcm.util.AttributeValues.s;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class Subscriptions {

  private static final Logger logger = LoggerFactory.getLogger(Subscriptions.class);

  private static final int USER_LENGTH = 16;
  private static final byte[] EMPTY_PROCESSOR = new byte[0];

  public static final String KEY_USER = "U";  // B  (Hash Key)
  public static final String KEY_PASSWORD = "P";  // B
  public static final String KEY_PROCESSOR_ID_CUSTOMER_ID = "PC"; // B (GSI Hash Key of `pc_to_u` index)
  public static final String KEY_CREATED_AT = "R";  // N
  public static final String KEY_SUBSCRIPTION_ID = "S";  // S
  public static final String KEY_SUBSCRIPTION_CREATED_AT = "T";  // N
  public static final String KEY_SUBSCRIPTION_LEVEL = "L";
  public static final String KEY_SUBSCRIPTION_LEVEL_CHANGED_AT = "V";  // N
  public static final String KEY_ACCESSED_AT = "A";  // N
  public static final String KEY_CANCELED_AT = "B";  // N
  public static final String KEY_CURRENT_PERIOD_ENDS_AT = "D";  // N

  public static final String INDEX_NAME = "pc_to_u";  // Hash Key "PC"

  public static class Record {

    public final byte[] user;
    public final byte[] password;
    public final Instant createdAt;
    @VisibleForTesting
    @Nullable
    ProcessorCustomer processorCustomer;
    @Nullable
    public String subscriptionId;
    public Instant subscriptionCreatedAt;
    public Long subscriptionLevel;
    public Instant subscriptionLevelChangedAt;
    public Instant accessedAt;
    public Instant canceledAt;
    public Instant currentPeriodEndsAt;

    private Record(byte[] user, byte[] password, Instant createdAt) {
      this.user = checkUserLength(user);
      this.password = Objects.requireNonNull(password);
      this.createdAt = Objects.requireNonNull(createdAt);
    }

    public static Record from(byte[] user, Map<String, AttributeValue> item) {
      Record record = new Record(
          user,
          item.get(KEY_PASSWORD).b().asByteArray(),
          getInstant(item, KEY_CREATED_AT));

      final Pair<PaymentProvider, String> processorCustomerId = getProcessorAndCustomer(item);
      if (processorCustomerId != null) {
        record.processorCustomer = new ProcessorCustomer(processorCustomerId.second(), processorCustomerId.first());
      }
      record.subscriptionId = getString(item, KEY_SUBSCRIPTION_ID);
      record.subscriptionCreatedAt = getInstant(item, KEY_SUBSCRIPTION_CREATED_AT);
      record.subscriptionLevel = getLong(item, KEY_SUBSCRIPTION_LEVEL);
      record.subscriptionLevelChangedAt = getInstant(item, KEY_SUBSCRIPTION_LEVEL_CHANGED_AT);
      record.accessedAt = getInstant(item, KEY_ACCESSED_AT);
      record.canceledAt = getInstant(item, KEY_CANCELED_AT);
      record.currentPeriodEndsAt = getInstant(item, KEY_CURRENT_PERIOD_ENDS_AT);
      return record;
    }

    public Optional<ProcessorCustomer> getProcessorCustomer() {
      return Optional.ofNullable(processorCustomer);
    }

    /**
     * Extracts the active processor and customer from a single attribute value in the given item.
     * <p>
     * Until existing data is migrated, this may return {@code null}.
     */
    @Nullable
    private static Pair<PaymentProvider, String> getProcessorAndCustomer(Map<String, AttributeValue> item) {

      final AttributeValue attributeValue = item.get(KEY_PROCESSOR_ID_CUSTOMER_ID);

      if (attributeValue == null) {
        // temporarily allow null values
        return null;
      }

      final byte[] processorAndCustomerId = attributeValue.b().asByteArray();
      final byte processorId = processorAndCustomerId[0];

      final PaymentProvider processor = PaymentProvider.forId(processorId);
      if (processor == null) {
        throw new IllegalStateException("unknown processor id: " + processorId);
      }

      final String customerId = new String(processorAndCustomerId, 1, processorAndCustomerId.length - 1,
          StandardCharsets.UTF_8);

      return new Pair<>(processor, customerId);
    }

    private static String getString(Map<String, AttributeValue> item, String key) {
      AttributeValue attributeValue = item.get(key);
      if (attributeValue == null) {
        return null;
      }
      return attributeValue.s();
    }

    private static Long getLong(Map<String, AttributeValue> item, String key) {
      AttributeValue attributeValue = item.get(key);
      if (attributeValue == null || attributeValue.n() == null) {
        return null;
      }
      return Long.valueOf(attributeValue.n());
    }

    private static Instant getInstant(Map<String, AttributeValue> item, String key) {
      AttributeValue attributeValue = item.get(key);
      if (attributeValue == null || attributeValue.n() == null) {
        return null;
      }
      return Instant.ofEpochSecond(Long.parseLong(attributeValue.n()));
    }
  }

  private final String table;
  private final DynamoDbAsyncClient client;

  public Subscriptions(
      @Nonnull String table,
      @Nonnull DynamoDbAsyncClient client) {
    this.table = Objects.requireNonNull(table);
    this.client = Objects.requireNonNull(client);
  }

  /**
   * Looks in the GSI for a record with the given customer id and returns the user id.
   */
  public CompletableFuture<byte[]> getSubscriberUserByProcessorCustomer(ProcessorCustomer processorCustomer) {
    QueryRequest query = QueryRequest.builder()
        .tableName(table)
        .indexName(INDEX_NAME)
        .keyConditionExpression("#processor_customer_id = :processor_customer_id")
        .projectionExpression("#user")
        .expressionAttributeNames(Map.of(
            "#processor_customer_id", KEY_PROCESSOR_ID_CUSTOMER_ID,
            "#user", KEY_USER))
        .expressionAttributeValues(Map.of(
            ":processor_customer_id", b(processorCustomer.toDynamoBytes())))
        .build();
    return client.query(query).thenApply(queryResponse -> {
      int count = queryResponse.count();
      if (count == 0) {
        return null;
      } else if (count > 1) {
        logger.error("expected invariant of 1-1 subscriber-customer violated for customer {} ({})",
            processorCustomer.customerId(), processorCustomer.processor());
        throw new IllegalStateException(
            "expected invariant of 1-1 subscriber-customer violated for customer " + processorCustomer);
      } else {
        Map<String, AttributeValue> result = queryResponse.items().getFirst();
        return result.get(KEY_USER).b().asByteArray();
      }
    });
  }

  public static class GetResult {

    public static final GetResult NOT_STORED = new GetResult(Type.NOT_STORED, null);
    public static final GetResult PASSWORD_MISMATCH = new GetResult(Type.PASSWORD_MISMATCH, null);

    public enum Type {
      NOT_STORED,
      PASSWORD_MISMATCH,
      FOUND
    }

    public final Type type;
    public final Record record;

    private GetResult(Type type, Record record) {
      this.type = type;
      this.record = record;
    }

    public static GetResult found(Record record) {
      return new GetResult(Type.FOUND, record);
    }
  }

  /**
   * Looks up a record with the given {@code user} and validates the {@code hmac} before returning it.
   */
  public CompletableFuture<GetResult> get(byte[] user, byte[] hmac) {
    return getUser(user).thenApply(getItemResponse -> {
      if (!getItemResponse.hasItem()) {
        return GetResult.NOT_STORED;
      }

      Record record = Record.from(user, getItemResponse.item());
      if (!MessageDigest.isEqual(hmac, record.password)) {
        return GetResult.PASSWORD_MISMATCH;
      }
      return GetResult.found(record);
    });
  }

  private CompletableFuture<GetItemResponse> getUser(byte[] user) {
    checkUserLength(user);

    GetItemRequest request = GetItemRequest.builder()
        .consistentRead(Boolean.TRUE)
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .build();

    return client.getItem(request);
  }

  public CompletableFuture<Record> create(byte[] user, byte[] password, Instant createdAt) {
    checkUserLength(user);

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .returnValues(ReturnValue.ALL_NEW)
        .conditionExpression("attribute_not_exists(#user) OR #password = :password")
        .updateExpression("SET "
            + "#password = if_not_exists(#password, :password), "
            + "#created_at = if_not_exists(#created_at, :created_at), "
            + "#accessed_at = if_not_exists(#accessed_at, :accessed_at)"
        )
        .expressionAttributeNames(Map.of(
            "#user", KEY_USER,
            "#password", KEY_PASSWORD,
            "#created_at", KEY_CREATED_AT,
            "#accessed_at", KEY_ACCESSED_AT)
        )
        .expressionAttributeValues(Map.of(
            ":password", b(password),
            ":created_at", n(createdAt.getEpochSecond()),
            ":accessed_at", n(createdAt.getEpochSecond()))
        )
        .build();
    return client.updateItem(request).handle((updateItemResponse, throwable) -> {
      if (throwable != null) {
        if (Throwables.getRootCause(throwable) instanceof ConditionalCheckFailedException) {
          return null;
        }
        Throwables.throwIfUnchecked(throwable);
        throw new CompletionException(throwable);
      }

      return Record.from(user, updateItemResponse.attributes());
    });
  }

  /**
   * Sets the processor and customer ID for the given user record.
   *
   * @return the user record.
   */
  public CompletableFuture<Record> setProcessorAndCustomerId(Record userRecord,
      ProcessorCustomer activeProcessorCustomer, Instant updatedAt) {

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(userRecord.user)))
        .returnValues(ReturnValue.ALL_NEW)
        .conditionExpression("attribute_not_exists(#processor_customer_id)")
        .updateExpression("SET "
            + "#processor_customer_id = :processor_customer_id, "
            + "#accessed_at = :accessed_at"
        )
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#processor_customer_id", KEY_PROCESSOR_ID_CUSTOMER_ID
        ))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(updatedAt.getEpochSecond()),
            ":processor_customer_id", b(activeProcessorCustomer.toDynamoBytes())
        )).build();

    return client.updateItem(request)
        .thenApply(updateItemResponse -> Record.from(userRecord.user, updateItemResponse.attributes()))
        .exceptionallyCompose(throwable -> {
          if (Throwables.getRootCause(throwable) instanceof ConditionalCheckFailedException) {
            throw new ClientErrorException(Response.Status.CONFLICT);
          }
          Throwables.throwIfUnchecked(throwable);
          throw new CompletionException(throwable);
        });
  }

  /**
   * Associate an IAP subscription with a subscriberId.
   * <p>
   * IAP subscriptions do not have a distinction between customerId and subscriptionId, so they should both be set
   * simultaneously with this method instead of calling {@link #setProcessorAndCustomerId},
   * {@link #subscriptionCreated}, and {@link #subscriptionLevelChanged}.
   *
   * @param record            The record to update
   * @param processorCustomer The processorCustomer. The processor component must match the existing processor, if the
   *                          record already has one.
   * @param subscriptionId    The subscriptionId. For IAP subscriptions, the subscriptionId should match the
   *                          customerId.
   * @param level             The corresponding level for this subscription
   * @param updatedAt         The time of this update
   * @return A stage that completes once the record has been updated
   */
  public CompletableFuture<Void> setIapPurchase(
      final Record record,
      final ProcessorCustomer processorCustomer,
      final String subscriptionId,
      final long level,
      final Instant updatedAt) {
    if (record.processorCustomer != null && record.processorCustomer.processor() != processorCustomer.processor()) {
      throw new IllegalArgumentException("cannot change processor on existing subscription");
    }
    final byte[] oldProcessorCustomerBytes = record.processorCustomer != null
        ? record.processorCustomer.toDynamoBytes()
        : EMPTY_PROCESSOR;

    final UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(record.user)))
        .returnValues(ReturnValue.ALL_NEW)
        .conditionExpression(
            "attribute_not_exists(#processor_customer_id) OR #processor_customer_id = :old_processor_customer_id")
        .updateExpression("SET "
            + "#processor_customer_id = :processor_customer_id, "
            + "#accessed_at = :accessed_at, "
            + "#subscription_id = :subscription_id, "
            + "#subscription_level = :subscription_level, "
            + "#subscription_created_at = if_not_exists(#subscription_created_at, :subscription_created_at), "
            + "#subscription_level_changed_at = :subscription_level_changed_at "
            + "REMOVE #canceled_at")
        .expressionAttributeNames(Map.of(
            "#processor_customer_id", KEY_PROCESSOR_ID_CUSTOMER_ID,
            "#accessed_at", KEY_ACCESSED_AT,
            "#subscription_id", KEY_SUBSCRIPTION_ID,
            "#subscription_level", KEY_SUBSCRIPTION_LEVEL,
            "#subscription_created_at", KEY_SUBSCRIPTION_CREATED_AT,
            "#subscription_level_changed_at", KEY_SUBSCRIPTION_LEVEL_CHANGED_AT,
            "#canceled_at", KEY_CANCELED_AT))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(updatedAt.getEpochSecond()),
            ":processor_customer_id", b(processorCustomer.toDynamoBytes()),
            ":old_processor_customer_id", b(oldProcessorCustomerBytes),
            ":subscription_id", s(subscriptionId),
            ":subscription_level", n(level),
            ":subscription_created_at", n(updatedAt.getEpochSecond()),
            ":subscription_level_changed_at", n(updatedAt.getEpochSecond())))
        .build();

    return client.updateItem(request)
        .exceptionallyCompose(throwable -> {
          if (Throwables.getRootCause(throwable) instanceof ConditionalCheckFailedException) {
            throw new ClientErrorException(Response.Status.CONFLICT);
          }
          Throwables.throwIfUnchecked(throwable);
          throw new CompletionException(throwable);
        })
        .thenRun(Util.NOOP);
  }

  public CompletableFuture<Void> accessedAt(byte[] user, Instant accessedAt) {
    checkUserLength(user);

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET #accessed_at = :accessed_at")
        .expressionAttributeNames(Map.of("#accessed_at", KEY_ACCESSED_AT))
        .expressionAttributeValues(Map.of(":accessed_at", n(accessedAt.getEpochSecond())))
        .build();
    return client.updateItem(request).thenApply(updateItemResponse -> null);
  }

  public CompletableFuture<Void> setCanceledAt(byte[] user, Instant canceledAt) {
    checkUserLength(user);

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET "
            + "#accessed_at = :accessed_at, "
            + "#canceled_at = :canceled_at "
            + "REMOVE #subscription_id")
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#canceled_at", KEY_CANCELED_AT,
            "#subscription_id", KEY_SUBSCRIPTION_ID))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(canceledAt.getEpochSecond()),
            ":canceled_at", n(canceledAt.getEpochSecond())))
        .build();
    return client.updateItem(request).thenApply(updateItemResponse -> null);
  }

  public CompletableFuture<Void> subscriptionCreated(
      byte[] user, String subscriptionId, Instant subscriptionCreatedAt, long level) {
    checkUserLength(user);

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET "
            + "#accessed_at = :accessed_at, "
            + "#subscription_id = :subscription_id, "
            + "#subscription_created_at = :subscription_created_at, "
            + "#subscription_level = :subscription_level, "
            + "#subscription_level_changed_at = :subscription_level_changed_at "
            + "REMOVE #canceled_at")
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#subscription_id", KEY_SUBSCRIPTION_ID,
            "#subscription_created_at", KEY_SUBSCRIPTION_CREATED_AT,
            "#subscription_level", KEY_SUBSCRIPTION_LEVEL,
            "#subscription_level_changed_at", KEY_SUBSCRIPTION_LEVEL_CHANGED_AT,
            "#canceled_at", KEY_CANCELED_AT))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(subscriptionCreatedAt.getEpochSecond()),
            ":subscription_id", s(subscriptionId),
            ":subscription_created_at", n(subscriptionCreatedAt.getEpochSecond()),
            ":subscription_level", n(level),
            ":subscription_level_changed_at", n(subscriptionCreatedAt.getEpochSecond())))
        .build();
    return client.updateItem(request).thenApply(updateItemResponse -> null);
  }

  public CompletableFuture<Void> subscriptionLevelChanged(
      byte[] user, Instant subscriptionLevelChangedAt, long level, String subscriptionId) {
    checkUserLength(user);

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET "
            + "#accessed_at = :accessed_at, "
            + "#subscription_id = :subscription_id, "
            + "#subscription_level = :subscription_level, "
            + "#subscription_level_changed_at = :subscription_level_changed_at "
            + "REMOVE #canceled_at")
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#subscription_id", KEY_SUBSCRIPTION_ID,
            "#subscription_level", KEY_SUBSCRIPTION_LEVEL,
            "#subscription_level_changed_at", KEY_SUBSCRIPTION_LEVEL_CHANGED_AT,
            "#canceled_at", KEY_CANCELED_AT))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(subscriptionLevelChangedAt.getEpochSecond()),
            ":subscription_id", s(subscriptionId),
            ":subscription_level", n(level),
            ":subscription_level_changed_at", n(subscriptionLevelChangedAt.getEpochSecond())))
        .build();
    return client.updateItem(request).thenApply(updateItemResponse -> null);
  }

  private static byte[] checkUserLength(final byte[] user) {
    if (user.length != USER_LENGTH) {
      throw new IllegalArgumentException("user length is wrong; expected " + USER_LENGTH + "; was " + user.length);
    }
    return user;
  }
}
