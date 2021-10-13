/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult.Type.FOUND;
import static org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult.Type.NOT_STORED;
import static org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult.Type.PASSWORD_MISMATCH;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager.GetResult;
import org.whispersystems.textsecuregcm.storage.SubscriptionManager.Record;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class SubscriptionManagerTest {

  private static final long NOW_EPOCH_SECONDS = 1_500_000_000L;
  private static final String SUBSCRIPTIONS_TABLE_NAME = "subscriptions";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder().
      tableName(SUBSCRIPTIONS_TABLE_NAME).
      hashKey(SubscriptionManager.KEY_USER).
      attributeDefinition(AttributeDefinition.builder().
          attributeName(SubscriptionManager.KEY_USER).
          attributeType(ScalarAttributeType.B).
          build()).
      attributeDefinition(AttributeDefinition.builder().
          attributeName(SubscriptionManager.KEY_CUSTOMER_ID).
          attributeType(ScalarAttributeType.S).
          build()).
      globalSecondaryIndex(GlobalSecondaryIndex.builder().
          indexName("c_to_u").
          keySchema(KeySchemaElement.builder().
              attributeName(SubscriptionManager.KEY_CUSTOMER_ID).
              keyType(KeyType.HASH).
              build()).
          projection(Projection.builder().
              projectionType(ProjectionType.KEYS_ONLY).
              build()).
          provisionedThroughput(ProvisionedThroughput.builder().
              readCapacityUnits(20L).
              writeCapacityUnits(20L).
              build()).
          build()).
      build();

  byte[] user;
  byte[] password;
  String customer;
  Instant created;
  SubscriptionManager subscriptionManager;

  @BeforeEach
  void beforeEach() {
    user = getRandomBytes(16);
    password = getRandomBytes(16);
    customer = Base64.getEncoder().encodeToString(getRandomBytes(16));
    created = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);
    subscriptionManager = new SubscriptionManager(
        SUBSCRIPTIONS_TABLE_NAME, dynamoDbExtension.getDynamoDbAsyncClient());
  }

  @Test
  void testCreateOnlyOnce() {
    byte[] password1 = getRandomBytes(16);
    byte[] password2 = getRandomBytes(16);
    String customer1 = Base64.getEncoder().encodeToString(getRandomBytes(16));
    String customer2 = Base64.getEncoder().encodeToString(getRandomBytes(16));
    Instant created1 = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);
    Instant created2 = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);

    CompletableFuture<GetResult> getFuture = subscriptionManager.get(user, password1);
    assertThat(getFuture).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult.type).isEqualTo(NOT_STORED);
      assertThat(getResult.record).isNull();
    });

    getFuture = subscriptionManager.get(user, password2);
    assertThat(getFuture).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult.type).isEqualTo(NOT_STORED);
      assertThat(getResult.record).isNull();
    });

    CompletableFuture<SubscriptionManager.Record> createFuture =
        subscriptionManager.create(user, password1, customer1, created1);
    Consumer<Record> recordRequirements = checkFreshlyCreatedRecord(user, password1, customer1, created1);
    assertThat(createFuture).succeedsWithin(Duration.ofSeconds(3)).satisfies(recordRequirements);

    // password check fails so this should return null
    createFuture = subscriptionManager.create(user, password2, customer2, created2);
    assertThat(createFuture).succeedsWithin(Duration.ofSeconds(3)).isNull();

    // password check matches, but the record already exists so nothing should get updated
    createFuture = subscriptionManager.create(user, password1, customer2, created2);
    assertThat(createFuture).succeedsWithin(Duration.ofSeconds(3)).satisfies(recordRequirements);
  }

  @Test
  void testGet() {
    byte[] wrongUser = getRandomBytes(16);
    byte[] wrongPassword = getRandomBytes(16);
    assertThat(subscriptionManager.create(user, password, customer, created)).succeedsWithin(Duration.ofSeconds(3));

    assertThat(subscriptionManager.get(user, password)).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(checkFreshlyCreatedRecord(user, password, customer, created));
    });

    assertThat(subscriptionManager.get(user, wrongPassword)).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult.type).isEqualTo(PASSWORD_MISMATCH);
      assertThat(getResult.record).isNull();
    });

    assertThat(subscriptionManager.get(wrongUser, password)).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult.type).isEqualTo(NOT_STORED);
      assertThat(getResult.record).isNull();
    });
  }

  @Test
  void testLookupByCustomerId() {
    assertThat(subscriptionManager.create(user, password, customer, created)).succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.getSubscriberUserByStripeCustomerId(customer)).
        succeedsWithin(Duration.ofSeconds(3)).
        isEqualTo(user);
  }

  @Test
  void testCanceledAt() {
    Instant canceled = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 42);
    assertThat(subscriptionManager.create(user, password, customer, created)).succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.canceledAt(user, canceled)).succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.get(user, password)).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult).isNotNull();
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(record -> {
        assertThat(record.accessedAt).isEqualTo(canceled);
        assertThat(record.canceledAt).isEqualTo(canceled);
        assertThat(record.subscriptionId).isNull();
      });
    });
  }

  @Test
  void testSubscriptionCreated() {
    String subscriptionId = Base64.getEncoder().encodeToString(getRandomBytes(16));
    Instant subscriptionCreated = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);
    long level = 42;
    assertThat(subscriptionManager.create(user, password, customer, created)).succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.subscriptionCreated(user, subscriptionId, subscriptionCreated, level)).
        succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.get(user, password)).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult).isNotNull();
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(record -> {
        assertThat(record.accessedAt).isEqualTo(subscriptionCreated);
        assertThat(record.subscriptionId).isEqualTo(subscriptionId);
        assertThat(record.subscriptionCreatedAt).isEqualTo(subscriptionCreated);
        assertThat(record.subscriptionLevel).isEqualTo(level);
        assertThat(record.subscriptionLevelChangedAt).isEqualTo(subscriptionCreated);
      });
    });
  }

  @Test
  void testSubscriptionLevelChanged() {
    Instant at = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 500);
    long level = 1776;
    assertThat(subscriptionManager.create(user, password, customer, created)).succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.subscriptionLevelChanged(user, at, level)).succeedsWithin(Duration.ofSeconds(3));
    assertThat(subscriptionManager.get(user, password)).succeedsWithin(Duration.ofSeconds(3)).satisfies(getResult -> {
      assertThat(getResult).isNotNull();
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(record -> {
        assertThat(record.accessedAt).isEqualTo(at);
        assertThat(record.subscriptionLevelChangedAt).isEqualTo(at);
        assertThat(record.subscriptionLevel).isEqualTo(level);
      });
    });
  }

  private static byte[] getRandomBytes(int length) {
    byte[] result = new byte[length];
    SECURE_RANDOM.nextBytes(result);
    return result;
  }

  @Nonnull
  private static Consumer<Record> checkFreshlyCreatedRecord(
      byte[] user, byte[] password, String customer, Instant created) {
    return record -> {
      assertThat(record).isNotNull();
      assertThat(record.user).isEqualTo(user);
      assertThat(record.password).isEqualTo(password);
      assertThat(record.customerId).isEqualTo(customer);
      assertThat(record.createdAt).isEqualTo(created);
      assertThat(record.subscriptionId).isNull();
      assertThat(record.subscriptionCreatedAt).isNull();
      assertThat(record.subscriptionLevel).isNull();
      assertThat(record.subscriptionLevelChangedAt).isNull();
      assertThat(record.accessedAt).isEqualTo(created);
      assertThat(record.canceledAt).isNull();
      assertThat(record.currentPeriodEndsAt).isNull();
    };
  }
}
