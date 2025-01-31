/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisCommandExecutionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import io.lettuce.core.RedisException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.Pair;

class MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @ParameterizedTest
  @MethodSource
  void testInsert(final int count, final Map<ServiceIdentifier, List<Byte>> destinations) throws Exception {

    final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript = new MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final byte[] sharedMrmKey = MessagesCache.getSharedMrmKey(UUID.randomUUID());
    insertMrmScript.executeAsync(sharedMrmKey,
        MessagesCacheTest.generateRandomMrmMessage(destinations)).join();

    final int totalDevices = destinations.values().stream().mapToInt(List::size).sum();
    final long hashFieldCount = REDIS_CLUSTER_EXTENSION.getRedisCluster()
        .withBinaryCluster(conn -> conn.sync().hlen(sharedMrmKey));
    // + 1 because of "data" field
    assertEquals(1 + totalDevices, hashFieldCount);
  }

  public static List<Arguments> testInsert() {
    final Map<ServiceIdentifier, List<Byte>> singleAccount = Map.of(
        new AciServiceIdentifier(UUID.randomUUID()), List.of((byte) 1, (byte) 2));

    final List<Arguments> testCases = new ArrayList<>();
    testCases.add(Arguments.of(1, singleAccount));

    for (int j = 1000; j <= 30000; j += 1000) {

      final Map<Integer, List<Byte>> deviceLists = new HashMap<>();
      final Map<ServiceIdentifier, List<Byte>> manyAccounts = IntStream.range(0, j)
          .mapToObj(i -> {
            final int deviceCount = 1 + i % 5;
            final List<Byte> devices = deviceLists.computeIfAbsent(deviceCount, count -> IntStream.rangeClosed(1, count)
                .mapToObj(v -> (byte) v)
                .toList());

            return new Pair<>(new AciServiceIdentifier(UUID.randomUUID()), devices);
          })
          .collect(Collectors.toMap(Pair::first, Pair::second));

      testCases.add(Arguments.of(j, manyAccounts));
    }

    return testCases;
  }

  @Test
  void testInsertDuplicateKey() throws Exception {
    final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript = new MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final byte[] sharedMrmKey = MessagesCache.getSharedMrmKey(UUID.randomUUID());
    insertMrmScript.executeAsync(sharedMrmKey,
        MessagesCacheTest.generateRandomMrmMessage(new AciServiceIdentifier(UUID.randomUUID()), Device.PRIMARY_ID)).join();

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> insertMrmScript.executeAsync(sharedMrmKey,
            MessagesCacheTest.generateRandomMrmMessage(new AciServiceIdentifier(UUID.randomUUID()),
                Device.PRIMARY_ID)).join());

    assertInstanceOf(RedisException.class, completionException.getCause());
    assertTrue(completionException.getCause().getMessage()
        .contains(MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript.ERROR_KEY_EXISTS));
  }

}
