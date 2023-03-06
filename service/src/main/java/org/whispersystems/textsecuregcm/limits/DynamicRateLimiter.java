/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class DynamicRateLimiter implements RateLimiter {

  private final String name;

  private final Supplier<RateLimiterConfig> configResolver;

  private final ClusterLuaScript validateScript;

  private final FaultTolerantRedisCluster cluster;

  private final Clock clock;

  private final AtomicReference<Pair<RateLimiterConfig, RateLimiter>> currentHolder = new AtomicReference<>();


  public DynamicRateLimiter(
      final String name,
      final Supplier<RateLimiterConfig> configResolver,
      final ClusterLuaScript validateScript,
      final FaultTolerantRedisCluster cluster,
      final Clock clock) {
    this.name = requireNonNull(name);
    this.configResolver = requireNonNull(configResolver);
    this.validateScript = requireNonNull(validateScript);
    this.cluster = requireNonNull(cluster);
    this.clock = requireNonNull(clock);
  }

  @Override
  public void validate(final String key, final int amount) throws RateLimitExceededException {
    current().getRight().validate(key, amount);
  }

  @Override
  public CompletionStage<Void> validateAsync(final String key, final int amount) {
    return current().getRight().validateAsync(key, amount);
  }

  @Override
  public boolean hasAvailablePermits(final String key, final int permits) {
    return current().getRight().hasAvailablePermits(key, permits);
  }

  @Override
  public CompletionStage<Boolean> hasAvailablePermitsAsync(final String key, final int amount) {
    return current().getRight().hasAvailablePermitsAsync(key, amount);
  }

  @Override
  public void clear(final String key) {
    current().getRight().clear(key);
  }

  @Override
  public CompletionStage<Void> clearAsync(final String key) {
    return current().getRight().clearAsync(key);
  }

  @Override
  public RateLimiterConfig config() {
    return current().getLeft();
  }

  private Pair<RateLimiterConfig, RateLimiter> current() {
    final RateLimiterConfig cfg = configResolver.get();
    return currentHolder.updateAndGet(p -> p != null && p.getLeft().equals(cfg)
        ? p
        : Pair.of(cfg, new StaticRateLimiter(name, cfg, validateScript, cluster, clock))
    );
  }
}
