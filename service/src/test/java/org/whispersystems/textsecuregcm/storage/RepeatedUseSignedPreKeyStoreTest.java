/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Subscriber;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.paginators.QueryPublisher;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RepeatedUseSignedPreKeyStoreTest {

  private RepeatedUseSignedPreKeyStore keys;

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.REPEATED_USE_SIGNED_PRE_KEYS);

  @BeforeEach
  void setUp() {
    keys = new RepeatedUseSignedPreKeyStore(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.REPEATED_USE_SIGNED_PRE_KEYS.tableName());
  }

  @Test
  void storeFind() {
    assertEquals(Optional.empty(), keys.find(UUID.randomUUID(), 1).join());

    {
      final UUID identifier = UUID.randomUUID();
      final long deviceId = 1;
      final SignedPreKey signedPreKey = generateSignedPreKey();

      assertDoesNotThrow(() -> keys.store(identifier, deviceId, signedPreKey).join());
      assertEquals(Optional.of(signedPreKey), keys.find(identifier, deviceId).join());
    }

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Long, SignedPreKey> signedPreKeys = Map.of(
          1L, generateSignedPreKey(),
          2L, generateSignedPreKey()
      );

      assertDoesNotThrow(() -> keys.store(identifier, signedPreKeys).join());
      assertEquals(Optional.of(signedPreKeys.get(1L)), keys.find(identifier, 1).join());
      assertEquals(Optional.of(signedPreKeys.get(2L)), keys.find(identifier, 2).join());
    }
  }

  @Test
  void delete() {
    assertDoesNotThrow(() -> keys.delete(UUID.randomUUID()).join());

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Long, SignedPreKey> signedPreKeys = Map.of(
          1L, generateSignedPreKey(),
          2L, generateSignedPreKey()
      );

      keys.store(identifier, signedPreKeys).join();
      keys.delete(identifier, 1).join();

      assertEquals(Optional.empty(), keys.find(identifier, 1).join());
      assertEquals(Optional.of(signedPreKeys.get(2L)), keys.find(identifier, 2).join());
    }

    {
      final UUID identifier = UUID.randomUUID();
      final Map<Long, SignedPreKey> signedPreKeys = Map.of(
          1L, generateSignedPreKey(),
          2L, generateSignedPreKey()
      );

      keys.store(identifier, signedPreKeys).join();
      keys.delete(identifier).join();

      assertEquals(Optional.empty(), keys.find(identifier, 1).join());
      assertEquals(Optional.empty(), keys.find(identifier, 2).join());
    }
  }

  @Test
  void deleteWithError() {
    final DynamoDbAsyncClient mockClient = mock(DynamoDbAsyncClient.class);
    final QueryPublisher queryPublisher = mock(QueryPublisher.class);

    final SdkPublisher<Map<String, AttributeValue>> itemPublisher = new SdkPublisher<Map<String, AttributeValue>>() {
      final Flux<Map<String, AttributeValue>> items = Flux.just(
          Map.of(RepeatedUseSignedPreKeyStore.KEY_DEVICE_ID, AttributeValues.fromLong(1)),
          Map.of(RepeatedUseSignedPreKeyStore.KEY_DEVICE_ID, AttributeValues.fromLong(2)));

      @Override
      public void subscribe(final Subscriber<? super Map<String, AttributeValue>> subscriber) {
        items.subscribe(subscriber);
      }
    };

    when(queryPublisher.items()).thenReturn(itemPublisher);
    when(mockClient.queryPaginator(any(QueryRequest.class))).thenReturn(queryPublisher);

    final Exception deleteItemException = new IllegalArgumentException("OH NO");

    when(mockClient.deleteItem(any(DeleteItemRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()))
        .thenReturn(CompletableFuture.failedFuture(deleteItemException));

    final RepeatedUseSignedPreKeyStore keyStore = new RepeatedUseSignedPreKeyStore(mockClient,
        DynamoDbExtensionSchema.Tables.REPEATED_USE_SIGNED_PRE_KEYS.tableName());

    final CompletionException completionException =
        assertThrows(CompletionException.class, () -> keyStore.delete(UUID.randomUUID()).join());

    assertEquals(deleteItemException, completionException.getCause());
  }

  private static SignedPreKey generateSignedPreKey() {
    return KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR);
  }
}
