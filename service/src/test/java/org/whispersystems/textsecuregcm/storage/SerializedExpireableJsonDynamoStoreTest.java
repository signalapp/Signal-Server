/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class SerializedExpireableJsonDynamoStoreTest {

  static abstract class Tests<T> {

    private static final String TABLE_NAME = "test";
    private static final String KEY = "foo";

    static final Clock clock = Clock.systemUTC();

    interface Value {

      String v();
    }

    @RegisterExtension
    static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
        new DynamoDbExtension.RawSchema(
            TABLE_NAME,
            SerializedExpireableJsonDynamoStore.KEY_KEY,
            null,
            List.of(AttributeDefinition.builder()
                .attributeName(SerializedExpireableJsonDynamoStore.KEY_KEY)
                .attributeType(ScalarAttributeType.S)
                .build()),
            List.of(),
            List.of()));

    private SerializedExpireableJsonDynamoStore<T> store;

    abstract SerializedExpireableJsonDynamoStore<T> getStore(final DynamoDbAsyncClient dynamoDbClient,
        final String tableName);

    abstract T testValue(final String v);

    abstract T maybeExpiredTestValue(final String v);

    @BeforeEach
    void setUp() {
      store = getStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(), TABLE_NAME);
    }

    @Test
    void testStoreAndFind() throws Exception {
      assertEquals(Optional.empty(), store.findForKey(KEY).get(1, TimeUnit.SECONDS));

      final T original = testValue("1234");
      final T second = testValue("5678");

      store.insert(KEY, original).get(1, TimeUnit.SECONDS);
      {
        final Optional<T> maybeValue = store.findForKey(KEY).get(1, TimeUnit.SECONDS);

        assertTrue(maybeValue.isPresent());
        assertEquals(original, maybeValue.get());
      }

      assertThrows(Exception.class, () -> store.insert(KEY, second).get(1, TimeUnit.SECONDS));

      assertDoesNotThrow(() -> store.update(KEY, second).get(1, TimeUnit.SECONDS));
      {
        final Optional<T> maybeValue = store.findForKey(KEY).get(1, TimeUnit.SECONDS);

        assertTrue(maybeValue.isPresent());
        assertEquals(second, maybeValue.get());
      }
    }

    @Test
    void testRemove() throws Exception {
      assertEquals(Optional.empty(), store.findForKey(KEY).get(1, TimeUnit.SECONDS));

      store.insert(KEY, testValue("1234")).get(1, TimeUnit.SECONDS);
      assertTrue(store.findForKey(KEY).get(1, TimeUnit.SECONDS).isPresent());

      store.remove(KEY).get(1, TimeUnit.SECONDS);
      assertFalse(store.findForKey(KEY).get(1, TimeUnit.SECONDS).isPresent());

      final T v = maybeExpiredTestValue("1234");
      store.insert(KEY, v).get(1, TimeUnit.SECONDS);

      assertEquals(v instanceof SerializedExpireableJsonDynamoStore.Expireable,
          store.findForKey(KEY).get(1, TimeUnit.SECONDS).isEmpty());
    }

  }

  record Expires(String v, long timestamp) implements SerializedExpireableJsonDynamoStore.Expireable, Tests.Value {

    static final Duration EXPIRATION = Duration.ofSeconds(30);

    @Override
    public long getExpirationEpochSeconds() {
      return Instant.ofEpochMilli(timestamp()).plus(EXPIRATION).getEpochSecond();
    }
  }

  @Nested
  class Expireable extends Tests<Expires> {

    class ExpiresStore extends SerializedExpireableJsonDynamoStore<Expires> {

      public ExpiresStore(final DynamoDbAsyncClient dynamoDbClient, final String tableName) {
        super(dynamoDbClient, tableName, clock);
      }
    }

    private static final long VALID_TIMESTAMP = Instant.now().toEpochMilli();
    private static final long EXPIRED_TIMESTAMP = Instant.now().minus(Expires.EXPIRATION).minus(
        Duration.ofHours(1)).toEpochMilli();

    @Override
    SerializedExpireableJsonDynamoStore<Expires> getStore(final DynamoDbAsyncClient dynamoDbClient,
        final String tableName) {
      return new ExpiresStore(dynamoDbClient, tableName);
    }

    @Override
    Expires testValue(final String v) {
      return new Expires(v, VALID_TIMESTAMP);
    }

    @Override
    Expires maybeExpiredTestValue(final String v) {
      return new Expires(v, EXPIRED_TIMESTAMP);
    }
  }

  record DoesNotExpire(String v) implements Tests.Value {

  }


  @Nested
  class NotExpireable extends Tests<DoesNotExpire> {

    class DoesNotExpireStore extends SerializedExpireableJsonDynamoStore<DoesNotExpire> {

      public DoesNotExpireStore(final DynamoDbAsyncClient dynamoDbClient, final String tableName) {
        super(dynamoDbClient, tableName, clock);
      }
    }

    @Override
    SerializedExpireableJsonDynamoStore<DoesNotExpire> getStore(final DynamoDbAsyncClient dynamoDbClient,
        final String tableName) {
      return new DoesNotExpireStore(dynamoDbClient, tableName);
    }

    @Override
    DoesNotExpire testValue(final String v) {
      return new DoesNotExpire(v);
    }

    @Override
    DoesNotExpire maybeExpiredTestValue(final String v) {
      return new DoesNotExpire(v);
    }
  }

}
