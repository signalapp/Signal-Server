/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * Timers for operations that may span machines or requests and require a persistently stored timer start itme
 */
public class PersistentTimer {

  private static final Logger logger = LoggerFactory.getLogger(PersistentTimer.class);

  private static String TIMER_NAMESPACE = "persistent_timer";
  @VisibleForTesting
  static final Duration TIMER_TTL = Duration.ofHours(1);

  private final FaultTolerantRedisClusterClient redisClient;
  private final Clock clock;


  public PersistentTimer(final FaultTolerantRedisClusterClient redisClient, final Clock clock) {
    this.redisClient = redisClient;
    this.clock = clock;
  }

  public class Sample {

    private final Instant start;
    private final String redisKey;

    public Sample(final Instant start, final String redisKey) {
      this.start = start;
      this.redisKey = redisKey;
    }

    /**
     * Stop the timer, recording the duration between now and the first call to start. This deletes the persistent timer.
     *
     * @param timer The micrometer timer to record the duration to
     * @return A future that completes when the resources associated with the persistent timer have been destroyed
     */
    public CompletableFuture<Void> stop(Timer timer) {
      Duration duration = Duration.between(start, clock.instant());
      timer.record(duration);
      return redisClient.withCluster(connection -> connection.async().del(redisKey))
          .toCompletableFuture()
          .thenRun(Util.NOOP);
    }
  }

  /**
   * Start the timer if a timer with the provided namespaced key has not already been started, otherwise return the
   * existing sample.
   *
   * @param namespace A namespace prefix to use for the timer
   * @param key The unique key within the namespace that identifies the timer
   * @return A future that completes with a {@link Sample} that can later be used to record the final duration.
   */
  public CompletableFuture<Sample> start(final String namespace, final String key) {
    final Instant now = clock.instant();
    final String redisKey = redisKey(namespace, key);

    return redisClient.withCluster(connection ->
            connection.async().setGet(redisKey, String.valueOf(now.getEpochSecond()), SetArgs.Builder.nx().ex(TIMER_TTL)))
        .toCompletableFuture()
        .thenApply(serialized -> new Sample(parseStoredTimestamp(serialized).orElse(now), redisKey));
  }

  @VisibleForTesting
  String redisKey(final String namespace, final String key) {
    return String.format("%s::%s::%s", TIMER_NAMESPACE, namespace, key);
  }

  private static Optional<Instant> parseStoredTimestamp(final @Nullable String serialized) {
    return Optional
        .ofNullable(serialized)
        .flatMap(s -> {
          try {
            return Optional.of(Long.parseLong(s));
          } catch (NumberFormatException e) {
            logger.warn("Failed to parse stored timestamp {}", s, e);
            return Optional.empty();
          }
        })
        .map(Instant::ofEpochSecond);
  }

}
