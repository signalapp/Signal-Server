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
import net.spy.memcached.MemcachedClient;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.util.Constants;

import static com.codahale.metrics.MetricRegistry.name;

public class RateLimiter {

  private final Meter           meter;
  private final MemcachedClient memcachedClient;
  private final String          name;
  private final int             bucketSize;
  private final double          leakRatePerMillis;

  public RateLimiter(MemcachedClient memcachedClient, String name,
                     int bucketSize, double leakRatePerMinute)
  {
    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

    this.meter             = metricRegistry.meter(name(getClass(), name, "exceeded"));
    this.memcachedClient   = memcachedClient;
    this.name              = name;
    this.bucketSize        = bucketSize;
    this.leakRatePerMillis = leakRatePerMinute / (60.0 * 1000.0);
  }

  public void validate(String key, int amount) throws RateLimitExceededException {
    LeakyBucket bucket = getBucket(key);

    if (bucket.add(amount)) {
      setBucket(key, bucket);
    } else {
      meter.mark();
      throw new RateLimitExceededException(key + " , " + amount);
    }
  }

  public void validate(String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  private void setBucket(String key, LeakyBucket bucket) {
    memcachedClient.set(getBucketName(key),
                        (int)Math.ceil((bucketSize / leakRatePerMillis) / 1000), bucket);
  }

  private LeakyBucket getBucket(String key) {
    LeakyBucket bucket = (LeakyBucket)memcachedClient.get(getBucketName(key));

    if (bucket == null) {
      return new LeakyBucket(bucketSize, leakRatePerMillis);
    } else {
      return bucket;
    }
  }

  private String getBucketName(String key) {
    return LeakyBucket.class.getSimpleName() + name + key;
  }
}
