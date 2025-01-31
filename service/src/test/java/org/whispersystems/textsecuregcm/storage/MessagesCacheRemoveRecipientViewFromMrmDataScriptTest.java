/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lettuce.core.cluster.SlotHash;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.Pair;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

class MessagesCacheRemoveRecipientViewFromMrmDataScriptTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @ParameterizedTest
  @MethodSource
  void testUpdateSingleKey(final Map<ServiceIdentifier, List<Byte>> destinations) throws Exception {

    final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript = new MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final byte[] sharedMrmKey = MessagesCache.getSharedMrmKey(UUID.randomUUID());
    insertMrmScript.executeAsync(sharedMrmKey, MessagesCacheTest.generateRandomMrmMessage(destinations)).join();

    final MessagesCacheRemoveRecipientViewFromMrmDataScript removeRecipientViewFromMrmDataScript = new MessagesCacheRemoveRecipientViewFromMrmDataScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final long keysRemoved = Objects.requireNonNull(Flux.fromIterable(destinations.entrySet())
        .flatMap(e -> Flux.fromStream(e.getValue().stream().map(deviceId -> Tuples.of(e.getKey(), deviceId))))
        .flatMap(serviceIdentifierByteTuple -> removeRecipientViewFromMrmDataScript.execute(List.of(sharedMrmKey),
            serviceIdentifierByteTuple.getT1(), serviceIdentifierByteTuple.getT2()))
        .reduce(Long::sum)
        .block(Duration.ofSeconds(35)));

    assertEquals(1, keysRemoved);

    final long keyExists = REDIS_CLUSTER_EXTENSION.getRedisCluster()
        .withBinaryCluster(conn -> conn.sync().exists(sharedMrmKey));
    assertEquals(0, keyExists);
  }

  public static List<Map<ServiceIdentifier, List<Byte>>> testUpdateSingleKey() {
    final Map<ServiceIdentifier, List<Byte>> singleAccount = Map.of(
        new AciServiceIdentifier(UUID.randomUUID()), List.of((byte) 1, (byte) 2));

    final List<Map<ServiceIdentifier, List<Byte>>> testCases = new ArrayList<>();
    testCases.add(singleAccount);

    // Generate a more, from smallish to very large
    for (int j = 1000; j <= 81000; j *= 3) {

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

      testCases.add(manyAccounts);
    }

    return testCases;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 100, 1000, 10000})
  void testUpdateManyKeys(int keyCount) throws Exception {

    final List<byte[]> sharedMrmKeys = new ArrayList<>(keyCount);
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final byte deviceId = 1;

    for (int i = 0; i < keyCount; i++) {

      final MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript insertMrmScript = new MessagesCacheInsertSharedMultiRecipientPayloadAndViewsScript(
          REDIS_CLUSTER_EXTENSION.getRedisCluster());

      final byte[] sharedMrmKey = MessagesCache.getSharedMrmKey(UUID.randomUUID());
      insertMrmScript.executeAsync(sharedMrmKey,
          MessagesCacheTest.generateRandomMrmMessage(serviceIdentifier, deviceId)).join();

      sharedMrmKeys.add(sharedMrmKey);
    }

    final MessagesCacheRemoveRecipientViewFromMrmDataScript removeRecipientViewFromMrmDataScript = new MessagesCacheRemoveRecipientViewFromMrmDataScript(
        REDIS_CLUSTER_EXTENSION.getRedisCluster());

    final long keysRemoved = Objects.requireNonNull(Flux.fromIterable(sharedMrmKeys)
        .collectMultimap(SlotHash::getSlot)
        .flatMapMany(slotsAndKeys -> Flux.fromIterable(slotsAndKeys.values()))
        .flatMap(keys -> removeRecipientViewFromMrmDataScript.execute(keys, serviceIdentifier, deviceId))
        .reduce(Long::sum)
        .block(Duration.ofSeconds(5)));

    assertEquals(sharedMrmKeys.size(), keysRemoved);
  }

}
