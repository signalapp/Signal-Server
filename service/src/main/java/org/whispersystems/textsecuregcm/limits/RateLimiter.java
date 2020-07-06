/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.limits;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.ScriptOutputType;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.experiment.Experiment;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class RateLimiter {

  private   final Meter                     meter;
  private   final Timer                     validateTimer;
  protected final ReplicatedJedisPool       cacheClient;
  protected final FaultTolerantRedisCluster cacheCluster;
  protected final String                    name;
  private   final int                       bucketSize;
  private   final double                    leakRatePerMillis;
  private   final Experiment                redisClusterExperiment;
  private   final LuaScript                 validateScript;
  private   final ClusterLuaScript          clusterValidateScript;

  public RateLimiter(ReplicatedJedisPool cacheClient, FaultTolerantRedisCluster cacheCluster, String name,
                     int bucketSize, double leakRatePerMinute) throws IOException {
    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

    this.meter                  = metricRegistry.meter(name(getClass(), name, "exceeded"));
    this.validateTimer          = metricRegistry.timer(name(getClass(), name, "validate"));
    this.cacheClient            = cacheClient;
    this.cacheCluster           = cacheCluster;
    this.name                   = name;
    this.bucketSize             = bucketSize;
    this.leakRatePerMillis      = leakRatePerMinute / (60.0 * 1000.0);
    this.redisClusterExperiment = new Experiment("RedisCluster", "RateLimiter", name);
    this.validateScript         = LuaScript.fromResource(cacheClient, "lua/validate_rate_limit.lua");
    this.clusterValidateScript  = ClusterLuaScript.fromResource(cacheCluster, "lua/validate_rate_limit.lua", ScriptOutputType.INTEGER);
  }

  public void validate(String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  public void validate(String key, int amount) throws RateLimitExceededException {
    validate(key, amount, System.currentTimeMillis());
  }

  @VisibleForTesting
  void validate(String key, int amount, final long currentTimeMillis) throws RateLimitExceededException {
    try (final Timer.Context ignored = validateTimer.time()) {
      final List<String> keys = List.of(getBucketName(key));
      final List<String> arguments = List.of(String.valueOf(bucketSize), String.valueOf(leakRatePerMillis), String.valueOf(currentTimeMillis), String.valueOf(amount));

      final Object result = validateScript.execute(keys.stream().map(k -> k.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList()),
                                                   arguments.stream().map(a -> a.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList()));

      redisClusterExperiment.compareSupplierResult(result, () -> clusterValidateScript.execute(keys, arguments));

      if (result == null) {
        meter.mark();
        throw new RateLimitExceededException(key + " , " + amount);
      }
    }
  }

  public void clear(String key) {
    try (Jedis jedis = cacheClient.getWriteResource()) {
      final String bucketName = getBucketName(key);

      jedis.del(bucketName);
      cacheCluster.useWriteCluster(connection -> connection.sync().del(bucketName));
    }
  }

  @VisibleForTesting
  String getBucketName(String key) {
    return "leaky_bucket::" + name + "::" + key;
  }
}
