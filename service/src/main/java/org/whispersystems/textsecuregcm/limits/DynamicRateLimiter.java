/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static java.util.Objects.requireNonNull;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;

public class DynamicRateLimiter implements RateLimiter {

  private final String name;
  private final Supplier<RateLimiterConfig> configResolver;

  private final ClusterLuaScript validateScript;

  private final FaultTolerantRedisClusterClient cluster;
  private final ScheduledExecutorService retryExecutor;

  private final Counter limitExceededCounter;

  private final Clock clock;

  private static final String RETRY_NAME = ResilienceUtil.name(DynamicRateLimiter.class);

  public DynamicRateLimiter(
      final String name,
      final Supplier<RateLimiterConfig> configResolver,
      final ClusterLuaScript validateScript,
      final FaultTolerantRedisClusterClient cluster,
      final ScheduledExecutorService retryExecutor,
      final Clock clock) {
    this.name = requireNonNull(name);
    this.configResolver = requireNonNull(configResolver);
    this.validateScript = requireNonNull(validateScript);
    this.cluster = requireNonNull(cluster);
    this.retryExecutor = requireNonNull(retryExecutor);
    this.clock = requireNonNull(clock);
    this.limitExceededCounter = Metrics.counter(MetricsUtil.name(getClass(), "exceeded"), "rateLimiterName", name);
  }

  @Override
  public void validate(final String key, final int amount) throws RateLimitExceededException {
    final RateLimiterConfig config = config();
    try {
      final long deficitPermitsAmount = executeValidateScript(config, key, amount, true);
      if (deficitPermitsAmount > 0) {
        limitExceededCounter.increment();
        final Duration retryAfter = Duration.ofMillis(
            (long) Math.ceil((double) deficitPermitsAmount / config.leakRatePerMillis()));
        throw new RateLimitExceededException(retryAfter);
      }
    } catch (final Exception e) {
      if (e instanceof RateLimitExceededException rateLimitExceededException) {
        throw rateLimitExceededException;
      }

      if (!config.failOpen()) {
        throw e;
      }
    }
  }

  @Override
  public CompletionStage<Void> validateAsync(final String key, final int amount) {
    final RateLimiterConfig config = config();

    return executeValidateScriptAsync(config, key, amount, true)
        .thenCompose(deficitPermitsAmount -> {
          if (deficitPermitsAmount == 0) {
            return CompletableFuture.completedFuture((Void) null);
          }
          limitExceededCounter.increment();
          final Duration retryAfter = Duration.ofMillis(
              (long) Math.ceil((double) deficitPermitsAmount / config.leakRatePerMillis()));
          return CompletableFuture.failedFuture(new RateLimitExceededException(retryAfter));
        })
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof RateLimitExceededException rateLimitExceededException) {
            throw ExceptionUtils.wrap(rateLimitExceededException);
          }

          if (config.failOpen()) {
            return null;
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  @Override
  public boolean hasAvailablePermits(final String key, final int permits) {
    final RateLimiterConfig config = config();
    try {
      final long deficitPermitsAmount = executeValidateScript(config, key, permits, false);
      return deficitPermitsAmount == 0;
    } catch (final Exception e) {
      if (config.failOpen()) {
        return true;
      } else {
        throw e;
      }
    }
  }

  @Override
  public CompletionStage<Boolean> hasAvailablePermitsAsync(final String key, final int amount) {
    final RateLimiterConfig config = config();
    return executeValidateScriptAsync(config, key, amount, false)
        .thenApply(deficitPermitsAmount -> deficitPermitsAmount == 0)
        .exceptionally(throwable -> {
          if (config.failOpen()) {
            return true;
          }
          throw ExceptionUtils.wrap(throwable);
        });
  }

  @Override
  public void clear(final String key) {
    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeRunnable(() -> cluster.useCluster(connection -> connection.sync().del(bucketName(name, key))));
  }

  @Override
  public CompletionStage<Void> clearAsync(final String key) {
    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeCompletionStage(retryExecutor, () -> cluster.withCluster(connection -> connection.async().del(bucketName(name, key)))
            .thenRun(Util.NOOP));
  }

  @Override
  public RateLimiterConfig config() {
    return configResolver.get();
  }

  private long executeValidateScript(final RateLimiterConfig config, final String key, final int amount, final boolean applyChanges) {
    final List<String> keys = List.of(bucketName(name, key));
    final List<String> arguments = List.of(
        String.valueOf(config.bucketSize()),
        String.valueOf(config.leakRatePerMillis()),
        String.valueOf(clock.millis()),
        String.valueOf(amount),
        String.valueOf(applyChanges)
    );
    return (Long) validateScript.execute(keys, arguments);
  }

  private CompletionStage<Long> executeValidateScriptAsync(final RateLimiterConfig config, final String key, final int amount, final boolean applyChanges) {
    final List<String> keys = List.of(bucketName(name, key));
    final List<String> arguments = List.of(
        String.valueOf(config.bucketSize()),
        String.valueOf(config.leakRatePerMillis()),
        String.valueOf(clock.millis()),
        String.valueOf(amount),
        String.valueOf(applyChanges)
    );
    return validateScript.executeAsync(keys, arguments).thenApply(o -> (Long) o);
  }

  private static String bucketName(final String name, final String key) {
    return "leaky_bucket::" + name + "::" + key;
  }
}
