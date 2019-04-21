package org.whispersystems.textsecuregcm.limits;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;

public class LockingRateLimiter extends RateLimiter {

  private final Meter meter;

  public LockingRateLimiter(ReplicatedJedisPool cacheClient, String name, int bucketSize, double leakRatePerMinute) {
    super(cacheClient, name, bucketSize, leakRatePerMinute);

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
      jedis.del(getLockName(key));
    }
  }

  private boolean acquireLock(String key) {
    try (Jedis jedis = cacheClient.getWriteResource()) {
      return jedis.set(getLockName(key), "L", "NX", "EX", 10) != null;
    }
  }

  private String getLockName(String key) {
    return "leaky_lock::" + name + "::" + key;
  }


}
