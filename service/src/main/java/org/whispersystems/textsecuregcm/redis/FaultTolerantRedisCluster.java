/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;

public interface FaultTolerantRedisCluster {

  void shutdown();

  String getName();

  void useCluster(Consumer<StatefulRedisClusterConnection<String, String>> consumer);

  <T> T withCluster(Function<StatefulRedisClusterConnection<String, String>, T> function);

  void useBinaryCluster(Consumer<StatefulRedisClusterConnection<byte[], byte[]>> consumer);

  <T> T withBinaryCluster(Function<StatefulRedisClusterConnection<byte[], byte[]>, T> function);

  <T> Publisher<T> withBinaryClusterReactive(
      Function<StatefulRedisClusterConnection<byte[], byte[]>, Publisher<T>> function);

  <K, V> void useConnection(StatefulRedisClusterConnection<K, V> connection,
      Consumer<StatefulRedisClusterConnection<K, V>> consumer);

  <T, K, V> T withConnection(StatefulRedisClusterConnection<K, V> connection,
      Function<StatefulRedisClusterConnection<K, V>, T> function);

  <T, K, V> Publisher<T> withConnectionReactive(StatefulRedisClusterConnection<K, V> connection,
      Function<StatefulRedisClusterConnection<K, V>, Publisher<T>> function);

  FaultTolerantPubSubConnection<String, String> createPubSubConnection();
}
