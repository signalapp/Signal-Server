/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.lettuce.core.RedisException;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class AbstractFaultTolerantPubSubConnection<K, V, C extends StatefulRedisPubSubConnection<K, V>> {

  private final String name;
  private final C pubSubConnection;

  private final Timer executeTimer;

  protected AbstractFaultTolerantPubSubConnection(final String name, final C pubSubConnection) {
    this.name = name;
    this.pubSubConnection = pubSubConnection;

    this.executeTimer = Metrics.timer(name(getClass(), "execute"), "clusterName", name + "-pubsub");
  }

  protected String getName() {
    return name;
  }

  public void usePubSubConnection(final Consumer<C> consumer) {
    try {
      executeTimer.record(() -> consumer.accept(pubSubConnection));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }

  public <T> T withPubSubConnection(final Function<C, T> function) {
    try {
      return executeTimer.record(() -> function.apply(pubSubConnection));
    } catch (final Throwable t) {
      if (t instanceof RedisException) {
        throw (RedisException) t;
      } else {
        throw new RedisException(t);
      }
    }
  }
}
