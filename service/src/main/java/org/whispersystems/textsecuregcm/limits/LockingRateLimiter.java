package org.whispersystems.textsecuregcm.limits;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.lettuce.core.SetArgs;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;

public class LockingRateLimiter extends RateLimiter {

  private final Meter meter;

  public LockingRateLimiter(ReplicatedJedisPool cacheClient, FaultTolerantRedisCluster cacheCluster, String name, int bucketSize, double leakRatePerMinute) {
    super(cacheClient, cacheCluster, name, bucketSize, leakRatePerMinute);

    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    this.meter = metricRegistry.meter(name(getClass(), name, "locked"));
  }

  @Override
  public void validate(String key, int amount) throws RateLimitExceededException {
    if (!acquireLock(key)) {
      meter.mark();
      throw new RateLimitExceededException("Locked");
    }

    try {
      super.validate(key, amount);
    } finally {
      releaseLock(key);
    }
  }

  @Override
  public void validate(String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  private void releaseLock(String key) {
    try (Jedis jedis = cacheClient.getWriteResource()) {
      final String lockName = getLockName(key);

      jedis.del(lockName);
      cacheCluster.useWriteCluster(connection -> connection.sync().del(lockName));
    }
  }

  private boolean acquireLock(String key) {
    try (Jedis jedis = cacheClient.getWriteResource()) {
      final String lockName = getLockName(key);

      final boolean acquiredLock = jedis.set(lockName, "L", "NX", "EX", 10) != null;

      if (acquiredLock) {
        // TODO Restore the NX flag when the cluster becomes the primary source of truth
        cacheCluster.useWriteCluster(connection -> connection.sync().set(lockName, "L", SetArgs.Builder.ex(10)));
      }

      return acquiredLock;
    }
  }

  private String getLockName(String key) {
    return "leaky_lock::" + name + "::" + key;
  }


}
