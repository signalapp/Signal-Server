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

import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.DirectoryReconciliationCache;
import org.whispersystems.textsecuregcm.util.Constants;
import redis.clients.jedis.Jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class ActiveUserCache {

  private static final String PREFIX                  = "active_user_";
  private static final String DATE_TO_REPORT_KEY      = PREFIX + "date_to_report";
  private static final String ACTIVE_WORKER_ID_KEY    = PREFIX + "worker_id";
  private static final String LAST_NUMBER_VISITED_KEY = PREFIX + "last_number_visited";

  private final ReplicatedJedisPool                          jedisPool;
  private final DirectoryReconciliationCache.UnlockOperation unlockOperation;

  public ActiveUserCache(ReplicatedJedisPool jedisPool) throws IOException {
    this.jedisPool       = jedisPool;
    this.unlockOperation = new DirectoryReconciliationCache.UnlockOperation(jedisPool);
  }

  public boolean claimActiveWorker(String workerId, long ttlMs) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return "OK".equals(jedis.set(ACTIVE_WORKER_ID_KEY, workerId, "NX", "PX", ttlMs));
    }
  }

  public void releaseActiveWorker(String workerId) {
    unlockOperation.unlock(ACTIVE_WORKER_ID_KEY, workerId);
  }

  public int getDateToReport(int initialDate) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      String value = jedis.get(DATE_TO_REPORT_KEY);
      return value == null ? initialDate : Integer.valueOf(value);
    }
  }

  public void setDateToReport(Integer date) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.set(DATE_TO_REPORT_KEY, date.toString());
    }
  }

  public Optional<String> getLastNumberVisited() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      return Optional.ofNullable(jedis.get(LAST_NUMBER_VISITED_KEY));
    }
  }

  public void setLastNumberVisited(Optional<String> number) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      if (number.isPresent()) {
        jedis.set(LAST_NUMBER_VISITED_KEY, number.get());
      } else {
        jedis.del(LAST_NUMBER_VISITED_KEY);
      }
    }
  }

  public void resetTallies(String platforms[], String intervals[]) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (String platform : platforms) {
        for (String interval : intervals) {
          jedis.set(tallyKey(platform, interval), "0");
        }
      }
    }
  }

  public void incrementTallies(String platform, String intervals[], long tallies[]) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (int i = 0; i < intervals.length; i++) {
        if (tallies[i] > 0) {
          jedis.incrBy(tallyKey(platform, intervals[i]), tallies[i]);
        }
      }
    }
  }

  public long[] getFinalTallies(String platform, String intervals[]) {
    try (Jedis jedis = jedisPool.getReadResource()) {
      long tallies[] = new long[intervals.length];
      for (int i = 0; i < intervals.length; i++) {
        tallies[i] = Long.valueOf(jedis.get(tallyKey(platform, intervals[i])));
      }
      return tallies;
    }
  }

  private String tallyKey(String platform, String intervalName) {
    return PREFIX + platform + "_tally_" + intervalName;
  }

}
