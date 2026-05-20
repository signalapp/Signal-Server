/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.whispersystems.textsecuregcm.storage.Subscriptions.GetResult.Type.FOUND;
import static org.whispersystems.textsecuregcm.storage.Subscriptions.GetResult.Type.NOT_STORED;
import static org.whispersystems.textsecuregcm.storage.Subscriptions.GetResult.Type.PASSWORD_MISMATCH;

import jakarta.ws.rs.ClientErrorException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.storage.Subscriptions.GetResult;
import org.whispersystems.textsecuregcm.storage.Subscriptions.Record;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.subscriptions.ProcessorCustomer;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class SubscriptionsTest {

  private static final long NOW_EPOCH_SECONDS = 1_500_000_000L;

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.SUBSCRIPTIONS);

  byte[] user;
  byte[] password;
  String customer;
  Instant created;
  Subscriptions subscriptions;

  @BeforeEach
  void beforeEach() {
    user = TestRandomUtil.nextBytes(16);
    password = TestRandomUtil.nextBytes(16);
    customer = Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(16));
    created = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);
    subscriptions = new Subscriptions(
        Tables.SUBSCRIPTIONS.tableName(), DYNAMO_DB_EXTENSION.getDynamoDbClient());
  }

  @Test
  void testCreateOnlyOnce() {
    byte[] password1 = TestRandomUtil.nextBytes(16);
    byte[] password2 = TestRandomUtil.nextBytes(16);
    Instant created1 = Instant.ofEpochSecond(NOW_EPOCH_SECONDS);
    Instant created2 = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);

    GetResult getResult = subscriptions.get(user, password1);
    assertThat(getResult.type).isEqualTo(NOT_STORED);
    assertThat(getResult.record).isNull();

    getResult = subscriptions.get(user, password2);
    assertThat(getResult.type).isEqualTo(NOT_STORED);
    assertThat(getResult.record).isNull();

    Subscriptions.Record create =
        subscriptions.create(user, password1, created1);
    Consumer<Record> recordRequirements = checkFreshlyCreatedRecord(user, password1, created1);
    assertThat(create).satisfies(recordRequirements);

    // password check fails so this should return null
    create = subscriptions.create(user, password2, created2);
    assertThat(create).isNull();

    // password check matches, but the record already exists so nothing should get updated
    create = subscriptions.create(user, password1, created2);
    assertThat(create).satisfies(recordRequirements);
  }

  @Test
  void testGet() {
    byte[] wrongUser = TestRandomUtil.nextBytes(16);
    byte[] wrongPassword = TestRandomUtil.nextBytes(16);

    subscriptions.create(user, password, created);

    assertThat(subscriptions.get(user, password)).satisfies(getResult -> {
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(checkFreshlyCreatedRecord(user, password, created));
    });

    assertThat(subscriptions.get(user, wrongPassword)).satisfies(getResult -> {
          assertThat(getResult.type).isEqualTo(PASSWORD_MISMATCH);
          assertThat(getResult.record).isNull();
        });

    assertThat(subscriptions.get(wrongUser, password)).satisfies(getResult -> {
          assertThat(getResult.type).isEqualTo(NOT_STORED);
          assertThat(getResult.record).isNull();
        });
  }

  @Test
  void testSetCustomerIdAndProcessor() {
    Instant subscriptionUpdated = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);
    assertThat(subscriptions.create(user, password, created)).isNotNull();

    final GetResult getUser = subscriptions.get(user, password);
    final Record userRecord = getUser.record;

    assertThat(subscriptions.setProcessorAndCustomerId(userRecord,
        new ProcessorCustomer(customer, PaymentProvider.STRIPE),
        subscriptionUpdated)).hasFieldOrPropertyWithValue("processorCustomer",
            Optional.of(new ProcessorCustomer(customer, PaymentProvider.STRIPE)));

    final Condition<Throwable> clientError409Condition = new Condition<>(e ->
        e instanceof ClientErrorException cee && cee.getResponse().getStatus() == 409, "Client error: 409");

    // changing the customer ID is not permitted

    assertThatThrownBy(() ->
        subscriptions.setProcessorAndCustomerId(userRecord,
            new ProcessorCustomer(customer + "1", PaymentProvider.STRIPE),
            subscriptionUpdated))
        .isInstanceOf(ClientErrorException.class)
        .satisfies(clientError409Condition);

    // calling setProcessorAndCustomerId() with the same customer ID is also an error
    assertThatThrownBy(() ->
        subscriptions.setProcessorAndCustomerId(userRecord,
            new ProcessorCustomer(customer, PaymentProvider.STRIPE),
            subscriptionUpdated))
        .isInstanceOf(ClientErrorException.class)
        .satisfies(clientError409Condition);

    assertThat(subscriptions.getSubscriberUserByProcessorCustomer(
        new ProcessorCustomer(customer, PaymentProvider.STRIPE)))
        .isEqualTo(user);
  }

  @Test
  void testLookupByCustomerId() {
    Instant subscriptionUpdated = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);
    subscriptions.create(user, password, created);

    final GetResult getUser = subscriptions.get(user, password);
    final Record userRecord = getUser.record;

    subscriptions.setProcessorAndCustomerId(userRecord,
        new ProcessorCustomer(customer, PaymentProvider.STRIPE),
        subscriptionUpdated);
    assertThat(subscriptions.getSubscriberUserByProcessorCustomer(
        new ProcessorCustomer(customer, PaymentProvider.STRIPE)))
        .isEqualTo(user);
  }

  @Test
  void testSetCanceledAt() {
    Instant canceled = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 42);
    subscriptions.create(user, password, created);
    subscriptions.setCanceledAt(user, canceled);

    assertThat(subscriptions.get(user, password)).satisfies(getResult -> {
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
    String subscriptionId = Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(16));
    Instant subscriptionCreated = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);
    long level = 42;
    subscriptions.create(user, password, created);
    subscriptions.subscriptionCreated(user, subscriptionId, subscriptionCreated, level);
    assertThat(subscriptions.get(user, password)).satisfies(getResult -> {
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
  void testSubscriptionCreatedClearCanceledAt() {
    String subscriptionId = Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(16));
    Instant subscriptionCreated = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 1);
    Instant canceledAt = subscriptionCreated.plusSeconds(1);
    long level = 42;
    subscriptions.create(user, password, created);
    subscriptions.subscriptionCreated(user, subscriptionId, subscriptionCreated, level);

    subscriptions.setCanceledAt(user, canceledAt);
    assertThat(subscriptions.get(user, password).record.canceledAt).isEqualTo(canceledAt);

    subscriptions.subscriptionCreated(user, subscriptionId, subscriptionCreated, level);

    assertThat(subscriptions.get(user, password)).satisfies(getResult -> {
      assertThat(getResult).isNotNull();
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(record -> {
        assertThat(record.accessedAt).isEqualTo(subscriptionCreated);
        assertThat(record.subscriptionId).isEqualTo(subscriptionId);
        assertThat(record.subscriptionCreatedAt).isEqualTo(subscriptionCreated);
        assertThat(record.subscriptionLevel).isEqualTo(level);
        assertThat(record.subscriptionLevelChangedAt).isEqualTo(subscriptionCreated);
        assertThat(record.canceledAt).isNull();
      });
    });
  }

  @Test
  void testSubscriptionLevelChanged() {
    Instant at = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 500);
    long level = 1776;
    String updatedSubscriptionId = "new";
    subscriptions.create(user, password, created);
    subscriptions.subscriptionCreated(user, "original", created, level - 1);
    subscriptions.subscriptionLevelChanged(user, at, level, updatedSubscriptionId);
    assertThat(subscriptions.get(user, password)).satisfies(getResult -> {
      assertThat(getResult).isNotNull();
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(record -> {
        assertThat(record.accessedAt).isEqualTo(at);
        assertThat(record.subscriptionLevelChangedAt).isEqualTo(at);
        assertThat(record.subscriptionLevel).isEqualTo(level);
        assertThat(record.subscriptionId).isEqualTo(updatedSubscriptionId);
      });
    });
  }

  @Test
  void testSubscriptionLevelChangedClearCanceledAt() {
    Instant at = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 500);
    Instant canceledAt = at.plusSeconds(100);
    long level = 1776;
    String updatedSubscriptionId = "new";
    subscriptions.create(user, password, created);
    subscriptions.subscriptionCreated(user, "original", created, level - 1);

    subscriptions.setCanceledAt(user, canceledAt);
    assertThat(subscriptions.get(user, password).record.canceledAt).isEqualTo(canceledAt);

    subscriptions.subscriptionLevelChanged(user, at, level, updatedSubscriptionId);
    assertThat(subscriptions.get(user, password)).satisfies(getResult -> {
      assertThat(getResult).isNotNull();
      assertThat(getResult.type).isEqualTo(FOUND);
      assertThat(getResult.record).isNotNull().satisfies(record -> {
        assertThat(record.accessedAt).isEqualTo(at);
        assertThat(record.subscriptionLevelChangedAt).isEqualTo(at);
        assertThat(record.subscriptionLevel).isEqualTo(level);
        assertThat(record.subscriptionId).isEqualTo(updatedSubscriptionId);
        assertThat(record.canceledAt).isNull();
      });
    });
  }

  @Test
  void testSetIapPurchase() {
    Instant at = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 500);
    long level = 100;

    ProcessorCustomer pc = new ProcessorCustomer("customerId", PaymentProvider.GOOGLE_PLAY_BILLING);
    Record record = assertThat(subscriptions.create(user, password, created)).isNotNull().actual();

    // Should be able to set a fresh subscription
    subscriptions.setIapPurchase(record, pc, "subscriptionId", level, at);

    record = subscriptions.get(user, password).record;
    assertThat(record.subscriptionLevel).isEqualTo(level);
    assertThat(record.subscriptionLevelChangedAt).isEqualTo(at);
    assertThat(record.subscriptionCreatedAt).isEqualTo(at);
    assertThat(record.getProcessorCustomer().orElseThrow()).isEqualTo(pc);

    // should be able to update the level
    Instant nextAt = at.plus(Duration.ofSeconds(10));
    long nextLevel = level + 1;
    subscriptions.setIapPurchase(record, pc, "subscriptionId", nextLevel, nextAt);

    record = subscriptions.get(user, password).record;
    assertThat(record.subscriptionLevel).isEqualTo(nextLevel);
    assertThat(record.subscriptionLevelChangedAt).isEqualTo(nextAt);
    assertThat(record.subscriptionCreatedAt).isEqualTo(at);
    assertThat(record.getProcessorCustomer().orElseThrow()).isEqualTo(pc);

    nextAt = nextAt.plus(Duration.ofSeconds(10));
    nextLevel = nextLevel + 1;

    pc = new ProcessorCustomer("newCustomerId", PaymentProvider.STRIPE);
    try {
      subscriptions.setIapPurchase(record, pc, "subscriptionId", nextLevel, nextAt);
      fail("should not be able to change the processor for an existing subscription record");
    } catch (IllegalArgumentException _) {
    }

    // should be able to change the customerId of an existing record if the processor matches
    pc = new ProcessorCustomer("newCustomerId", PaymentProvider.GOOGLE_PLAY_BILLING);
    subscriptions.setIapPurchase(record, pc, "subscriptionId", nextLevel, nextAt);

    record = subscriptions.get(user, password).record;
    assertThat(record.subscriptionLevel).isEqualTo(nextLevel);
    assertThat(record.subscriptionLevelChangedAt).isEqualTo(nextAt);
    assertThat(record.subscriptionCreatedAt).isEqualTo(at);
    assertThat(record.getProcessorCustomer().orElseThrow()).isEqualTo(pc);
  }

  @Test
  void testSetIapPurchaseClearCanceledAt() {
    Instant at = Instant.ofEpochSecond(NOW_EPOCH_SECONDS + 500);
    Instant canceledAt = at.plusSeconds(100);
    long level = 100;

    ProcessorCustomer pc = new ProcessorCustomer("customerId", PaymentProvider.GOOGLE_PLAY_BILLING);
    Record record = assertThat(subscriptions.create(user, password, created)).isNotNull().actual();

    // Should be able to set a fresh subscription
    subscriptions.setIapPurchase(record, pc, "subscriptionId", level, at);

    subscriptions.setCanceledAt(record.user, canceledAt);

    record = subscriptions.get(user, password).record;
    assertThat(record.canceledAt).isEqualTo(canceledAt);

    // should be able to update the level
    Instant nextAt = at.plus(Duration.ofSeconds(10));
    long nextLevel = level + 1;
    subscriptions.setIapPurchase(record, pc, "subscriptionId", nextLevel, nextAt);

    // Resetting the level should clear the "canceled at" timestamp
    record = subscriptions.get(user, password).record;
    assertThat(record.canceledAt).isNull();
  }

  @Test
  void testProcessorAndCustomerId() {
    final ProcessorCustomer processorCustomer =
        new ProcessorCustomer("abc", PaymentProvider.STRIPE);

    assertThat(processorCustomer.toDynamoBytes()).isEqualTo(new byte[]{1, 97, 98, 99});
  }

  @Nonnull
  private static Consumer<Record> checkFreshlyCreatedRecord(
      byte[] user, byte[] password, Instant created) {
    return record -> {
      assertThat(record).isNotNull();
      assertThat(record.user).isEqualTo(user);
      assertThat(record.password).isEqualTo(password);
      assertThat(record.processorCustomer).isNull();
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
