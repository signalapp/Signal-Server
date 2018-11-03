/**
 * Copyright (C) 2018 Open WhisperSystems
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationCache;
import org.whispersystems.textsecuregcm.util.Constants;
import redis.clients.jedis.Jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class ActiveUserCache {

  public static final int  DEFAULT_DATE = 2000_01_01;
  public static final long INITIAL_ID   = 0L;

  private static final Logger         logger  = LoggerFactory.getLogger(ActiveUserCache.class);
  private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private static final String PREFIX     = "active_user_";
  private static final String DATE_KEY   = PREFIX + "date";
  private static final String WORKER_KEY = PREFIX + "worker";
  private static final String ID_KEY     = PREFIX + "id";

  private static final String PLATFORM_IOS     = "ios";
  private static final String PLATFORM_ANDROID = "android";

  private static final String PLATFORMS[] = {PLATFORM_IOS, PLATFORM_ANDROID};
  private static final String INTERVALS[] = {"daily", "weekly", "monthly", "quarterly", "yearly"};

  private final ReplicatedJedisPool                          jedisPool;
  private final DirectoryReconciliationCache.UnlockOperation unlockOperation;

  public ActiveUserCache(ReplicatedJedisPool jedisPool) throws IOException {
    this.jedisPool       = jedisPool;
    this.unlockOperation = new DirectoryReconciliationCache.UnlockOperation(jedisPool);
  }

  public boolean claimActiveWorker(String workerId, long ttlMs) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return "OK".equals(jedis.set(WORKER_KEY, workerId, "NX", "PX", ttlMs));
    }
  }

  public void releaseActiveWorker(String workerId) {
    unlockOperation.unlock(WORKER_KEY, workerId);
  }

  public int getDate() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      String value = jedis.get(DATE_KEY);
      return value == null ? DEFAULT_DATE : Integer.valueOf(value);
    }
  }

  public void setDate(Integer date) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.set(DATE_KEY, date.toString());
    }
  }

  public Optional<Long> getId() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      String value = jedis.get(ID_KEY);
      return Optional.fromNullable(value == null ? null : Long.valueOf(value));
    }
  }

  public void setId(Optional<Long> id) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      if (id.isPresent()) {
        jedis.set(ID_KEY, id.get().toString());
      } else {
        jedis.del(ID_KEY);
      }
    }
  }

  public void resetTallies() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (String platform : PLATFORMS) {
        for (String interval : INTERVALS) {
          jedis.set(tallyKey(platform, interval), "0");
        }
      }
    }
  }

  public void incrementTallies(long ios[], long android[]) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (String platform : PLATFORMS) {
        long platformTallies[];
        switch (platform) {
        case PLATFORM_IOS:
          platformTallies = ios;
          break;
        case PLATFORM_ANDROID:
          platformTallies = android;
          break;
        default:
          throw new AssertionError("unknown platform" + platform);
        }
        for (int i = 0; i < INTERVALS.length; i++) {
          logger.debug(tallyKey(platform, INTERVALS[i]) + " " + platformTallies[i]);
          if (platformTallies[i] > 0)
            jedis.incrBy(tallyKey(platform, INTERVALS[i]), platformTallies[i]);
        }
      }
    }
  }

  public void registerTallies() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (String interval: INTERVALS) {
        int total = 0;
        for (String platform : PLATFORMS) {
          int tally = Integer.valueOf(jedis.get(tallyKey(platform, interval)));
          logger.debug(metricKey(platform, interval) + " " + tally);
          metrics.register(metricKey(platform, interval),
                           new Gauge<Integer>() {
                             @Override
                             public Integer getValue() { return tally; }
                           });
          total += tally;

        }

        final int finalTotal = total;
        logger.debug(metricKey(interval) + " " + finalTotal);
        metrics.register(metricKey(interval),
                         new Gauge<Integer>() {
                           @Override
                           public Integer getValue() { return finalTotal; }
                         });
      }
    }
  }

  private String metricKey(String platform, String interval) {
    return MetricRegistry.name(ActiveUserCache.class, interval + "_active_" + platform);
  }

  private String metricKey(String interval) {
    return MetricRegistry.name(ActiveUserCache.class, interval + "_active");
  }

  private String tallyKey(String platform, String intervalName) {
    return PREFIX + platform + "_tally_" + intervalName;
  }

}
