/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.util.AttributeValues.b;
import static org.whispersystems.textsecuregcm.util.AttributeValues.m;
import static org.whispersystems.textsecuregcm.util.AttributeValues.n;
import static org.whispersystems.textsecuregcm.util.AttributeValues.s;

import com.google.common.base.Throwables;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessor;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class SubscriptionManager {

  private static final Logger logger = LoggerFactory.getLogger(SubscriptionManager.class);

  private static final int USER_LENGTH = 16;

  public static final String KEY_USER = "U";  // B  (Hash Key)
  public static final String KEY_PASSWORD = "P";  // B
  @Deprecated
  public static final String KEY_CUSTOMER_ID = "C";  // S  (GSI Hash Key of `c_to_u` index)
  public static final String KEY_PROCESSOR_ID_CUSTOMER_ID = "PC"; // B (GSI Hash Key of `pc_to_u` index)
  public static final String KEY_CREATED_AT = "R";  // N
  public static final String KEY_PROCESSOR_CUSTOMER_IDS_MAP = "PCI"; // M
  public static final String KEY_SUBSCRIPTION_ID = "S";  // S
  public static final String KEY_SUBSCRIPTION_CREATED_AT = "T";  // N
  public static final String KEY_SUBSCRIPTION_LEVEL = "L";
  public static final String KEY_SUBSCRIPTION_LEVEL_CHANGED_AT = "V";  // N
  public static final String KEY_ACCESSED_AT = "A";  // N
  public static final String KEY_CANCELED_AT = "B";  // N
  public static final String KEY_CURRENT_PERIOD_ENDS_AT = "D";  // N

  public static final String INDEX_NAME = "c_to_u";  // Hash Key "C"

  public static class Record {

    public final byte[] user;
    public final byte[] password;
    public final Instant createdAt;
    public @Nullable String customerId;
    public @Nullable SubscriptionProcessor processor;
    public Map<SubscriptionProcessor, String> processorsToCustomerIds;
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
      Record self = new Record(
          user,
          item.get(KEY_PASSWORD).b().asByteArray(),
          getInstant(item, KEY_CREATED_AT));

      final Pair<SubscriptionProcessor, String> processorCustomerId = getProcessorAndCustomer(item);
      if (processorCustomerId != null) {
        self.customerId = processorCustomerId.second();
        self.processor = processorCustomerId.first();
      } else {
        // Until all existing data is migrated to KEY_PROCESSOR_ID_CUSTOMER_ID, fall back to KEY_CUSTOMER_ID
        self.customerId = getString(item, KEY_CUSTOMER_ID);
      }
      self.processorsToCustomerIds = getProcessorsToCustomerIds(item);
      self.subscriptionId = getString(item, KEY_SUBSCRIPTION_ID);
      self.subscriptionCreatedAt = getInstant(item, KEY_SUBSCRIPTION_CREATED_AT);
      self.subscriptionLevel = getLong(item, KEY_SUBSCRIPTION_LEVEL);
      self.subscriptionLevelChangedAt = getInstant(item, KEY_SUBSCRIPTION_LEVEL_CHANGED_AT);
      self.accessedAt = getInstant(item, KEY_ACCESSED_AT);
      self.canceledAt = getInstant(item, KEY_CANCELED_AT);
      self.currentPeriodEndsAt = getInstant(item, KEY_CURRENT_PERIOD_ENDS_AT);
      return self;
    }

    private static Map<SubscriptionProcessor, String> getProcessorsToCustomerIds(Map<String, AttributeValue> item) {
      final AttributeValue attributeValue = item.get(KEY_PROCESSOR_CUSTOMER_IDS_MAP);
      final Map<String, AttributeValue> attribute =
          attributeValue == null ? Collections.emptyMap() : attributeValue.m();

      final Map<SubscriptionProcessor, String> processorsToCustomerIds = new HashMap<>();
      attribute.forEach((processorName, customerId) ->
          processorsToCustomerIds.put(SubscriptionProcessor.valueOf(processorName), customerId.s()));

      return processorsToCustomerIds;
    }

    /**
     * Extracts the active processor and customer from a single attribute value in the given item.
     * <p>
     * Until existing data is migrated, this may return {@code null}.
     */
    @Nullable
    private static Pair<SubscriptionProcessor, String> getProcessorAndCustomer(Map<String, AttributeValue> item) {

      final AttributeValue attributeValue = item.get(KEY_PROCESSOR_ID_CUSTOMER_ID);

      if (attributeValue == null) {
        // temporarily allow null values
        return null;
      }

      final byte[] processorAndCustomerId = attributeValue.b().asByteArray();
      final byte processorId = processorAndCustomerId[0];

      final SubscriptionProcessor processor = SubscriptionProcessor.forId(processorId);
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

  public SubscriptionManager(
      @Nonnull String table,
      @Nonnull DynamoDbAsyncClient client) {
    this.table = Objects.requireNonNull(table);
    this.client = Objects.requireNonNull(client);
  }

  /**
   * Looks in the GSI for a record with the given customer id and returns the user id.
   */
  public CompletableFuture<byte[]> getSubscriberUserByStripeCustomerId(@Nonnull String customerId) {
    QueryRequest query = QueryRequest.builder()
        .tableName(table)
        .indexName(INDEX_NAME)
        .keyConditionExpression("#customer_id = :customer_id")
        .projectionExpression("#user")
        .expressionAttributeNames(Map.of(
            "#customer_id", KEY_CUSTOMER_ID,
            "#user", KEY_USER))
        .expressionAttributeValues(Map.of(
            ":customer_id", s(Objects.requireNonNull(customerId))))
        .build();
    return client.query(query).thenApply(queryResponse -> {
      int count = queryResponse.count();
      if (count == 0) {
        return null;
      } else if (count > 1) {
        logger.error("expected invariant of 1-1 subscriber-customer violated for customer {}", customerId);
        throw new IllegalStateException(
            "expected invariant of 1-1 subscriber-customer violated for customer " + customerId);
      } else {
        Map<String, AttributeValue> result = queryResponse.items().get(0);
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
            + "#accessed_at = if_not_exists(#accessed_at, :accessed_at), "
            + "#processors_to_customer_ids = if_not_exists(#processors_to_customer_ids, :initial_empty_map)"
        )
        .expressionAttributeNames(Map.of(
            "#user", KEY_USER,
            "#password", KEY_PASSWORD,
            "#created_at", KEY_CREATED_AT,
            "#accessed_at", KEY_ACCESSED_AT,
            "#processors_to_customer_ids", KEY_PROCESSOR_CUSTOMER_IDS_MAP)
        )
        .expressionAttributeValues(Map.of(
            ":password", b(password),
            ":created_at", n(createdAt.getEpochSecond()),
            ":accessed_at", n(createdAt.getEpochSecond()),
            ":initial_empty_map", m(Map.of()))
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
   * Updates the active processor and customer ID for the given user record.
   *
   * @return the updated user record.
   */
  public CompletableFuture<Record> updateProcessorAndCustomerId(Record userRecord,
      ProcessorCustomer activeProcessorCustomer, Instant updatedAt) {

    // Don’t attempt to modify the existing map, since it may be immutable, and we also don’t want to have side effects
    final Map<SubscriptionProcessor, String> allProcessorsAndCustomerIds = new HashMap<>(
        userRecord.processorsToCustomerIds);
    allProcessorsAndCustomerIds.put(activeProcessorCustomer.processor(), activeProcessorCustomer.customerId());

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(userRecord.user)))
        .returnValues(ReturnValue.ALL_NEW)
        .conditionExpression(
            // there is no customer attribute yet
            "attribute_not_exists(#customer_id) " +
                // OR this record doesn't have the new processor+customer attributes yet
                "OR (#customer_id = :customer_id " +
                "AND attribute_not_exists(#processor_customer_id) " +
                // TODO once all records are guaranteed to have the map, we can do a more targeted update
                //   "AND attribute_not_exists(#processors_to_customer_ids.#processor_name) " +
                "AND attribute_not_exists(#processors_to_customer_ids))"
        )
        .updateExpression("SET "
            + "#customer_id = :customer_id, "
            + "#processor_customer_id = :processor_customer_id, "
            // TODO once all records are guaranteed to have the map, we can do a more targeted update
            //  + "#processors_to_customer_ids.#processor_name = :customer_id, "
            + "#processors_to_customer_ids = :processors_and_customer_ids, "
            + "#accessed_at = :accessed_at"
        )
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#customer_id", KEY_CUSTOMER_ID,
            "#processor_customer_id", KEY_PROCESSOR_ID_CUSTOMER_ID,
            // TODO "#processor_name", activeProcessor.name(),
            "#processors_to_customer_ids", KEY_PROCESSOR_CUSTOMER_IDS_MAP
        ))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(updatedAt.getEpochSecond()),
            ":customer_id", s(activeProcessorCustomer.customerId()),
            ":processor_customer_id", b(activeProcessorCustomer.toDynamoBytes()),
            ":processors_and_customer_ids", m(createProcessorsToCustomerIdsAttributeMap(allProcessorsAndCustomerIds))
        )).build();

    return client.updateItem(request)
        .thenApply(updateItemResponse -> Record.from(userRecord.user, updateItemResponse.attributes()))
        .exceptionallyCompose(throwable -> {
          if (Throwables.getRootCause(throwable) instanceof ConditionalCheckFailedException) {
            return getUser(userRecord.user).thenApply(getItemResponse ->
                Record.from(userRecord.user, getItemResponse.item()));
          }
          Throwables.throwIfUnchecked(throwable);
          throw new CompletionException(throwable);
        });
  }

  private Map<String, AttributeValue> createProcessorsToCustomerIdsAttributeMap(
      Map<SubscriptionProcessor, String> allProcessorsAndCustomerIds) {
    final Map<String, AttributeValue> result = new HashMap<>();

    allProcessorsAndCustomerIds.forEach((processor, customerId) -> result.put(processor.name(), s(customerId)));

    return result;
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

  public CompletableFuture<Void> canceledAt(byte[] user, Instant canceledAt) {
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
            + "#subscription_level_changed_at = :subscription_level_changed_at")
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#subscription_id", KEY_SUBSCRIPTION_ID,
            "#subscription_created_at", KEY_SUBSCRIPTION_CREATED_AT,
            "#subscription_level", KEY_SUBSCRIPTION_LEVEL,
            "#subscription_level_changed_at", KEY_SUBSCRIPTION_LEVEL_CHANGED_AT))
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
      byte[] user, Instant subscriptionLevelChangedAt, long level) {
    checkUserLength(user);

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(table)
        .key(Map.of(KEY_USER, b(user)))
        .returnValues(ReturnValue.NONE)
        .updateExpression("SET "
            + "#accessed_at = :accessed_at, "
            + "#subscription_level = :subscription_level, "
            + "#subscription_level_changed_at = :subscription_level_changed_at")
        .expressionAttributeNames(Map.of(
            "#accessed_at", KEY_ACCESSED_AT,
            "#subscription_level", KEY_SUBSCRIPTION_LEVEL,
            "#subscription_level_changed_at", KEY_SUBSCRIPTION_LEVEL_CHANGED_AT))
        .expressionAttributeValues(Map.of(
            ":accessed_at", n(subscriptionLevelChangedAt.getEpochSecond()),
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
