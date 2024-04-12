/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import java.util.function.Consumer;
import java.util.function.Function;

public interface FaultTolerantPubSubConnection<K, V> {

  void usePubSubConnection(Consumer<StatefulRedisClusterPubSubConnection<K, V>> consumer);

  <T> T withPubSubConnection(Function<StatefulRedisClusterPubSubConnection<K, V>, T> function);

  void subscribeToClusterTopologyChangedEvents(Runnable eventHandler);
}
