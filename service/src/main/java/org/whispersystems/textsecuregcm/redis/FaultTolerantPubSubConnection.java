/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.retry.Retry;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class FaultTolerantPubSubConnection<K, V> extends AbstractFaultTolerantPubSubConnection<K, V, StatefulRedisPubSubConnection<K, V>> {

  protected FaultTolerantPubSubConnection(final String name,
      final StatefulRedisPubSubConnection<K, V> pubSubConnection,
      final Retry retry) {

    super(name, pubSubConnection, retry);
  }
}
