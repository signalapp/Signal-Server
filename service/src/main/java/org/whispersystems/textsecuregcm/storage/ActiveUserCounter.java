/*
 * Copyright (C) 2018 Open WhisperSystems
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
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.whispersystems.textsecuregcm.entities.ActiveUserTally;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import redis.clients.jedis.Jedis;

public class ActiveUserCounter extends AccountDatabaseCrawlerListener {

  private static final String TALLY_KEY         = "active_user_tally";

  private static final String PLATFORM_IOS     = "ios";
  private static final String PLATFORM_ANDROID = "android";

  private static final String INTERVALS[] = {"daily", "weekly", "monthly", "quarterly", "yearly"};

  private final MetricsFactory      metricsFactory;
  private final ReplicatedJedisPool jedisPool;
  private final ObjectMapper        mapper;

  public ActiveUserCounter(MetricsFactory metricsFactory, ReplicatedJedisPool jedisPool) {
    this.metricsFactory  = metricsFactory;
    this.jedisPool       = jedisPool;
    this.mapper          = SystemMapper.getMapper();
  }

  @Override
  public void onCrawlStart() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.del(TALLY_KEY);
    }
  }

  @Override
  public void onCrawlEnd(Optional<UUID> fromNumber) {
    MetricRegistry      metrics           = new MetricRegistry();
    long                intervalTallies[] = new long[INTERVALS.length];
    ActiveUserTally     activeUserTally   = getFinalTallies();
    Map<String, long[]> platforms         = activeUserTally.getPlatforms();

    platforms.forEach((platform, platformTallies) -> {
      for (int i = 0; i < INTERVALS.length; i++) {
        final long tally = platformTallies[i];
        metrics.register(metricKey(platform, INTERVALS[i]),
                         (Gauge<Long>) () -> tally);
        intervalTallies[i] += tally;
      }
    });

    Map<String, long[]> countries = activeUserTally.getCountries();
    countries.forEach((country, countryTallies) -> {
      for (int i = 0; i < INTERVALS.length; i++) {
        final long tally = countryTallies[i];
        metrics.register(metricKey(country, INTERVALS[i]),
                         (Gauge<Long>) () -> tally);
      }
    });

    for (int i = 0; i < INTERVALS.length; i++) {
      final long intervalTotal = intervalTallies[i];
      metrics.register(metricKey(INTERVALS[i]),
                       (Gauge<Long>) () -> intervalTotal);
    }

    for (ReporterFactory reporterFactory : metricsFactory.getReporters()) {
      reporterFactory.build(metrics).report();
    }
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromNumber, List<Account> chunkAccounts) {
    long nowDays  = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    long agoMs[]  = {TimeUnit.DAYS.toMillis(nowDays - 1),
                     TimeUnit.DAYS.toMillis(nowDays - 7),
                     TimeUnit.DAYS.toMillis(nowDays - 30),
                     TimeUnit.DAYS.toMillis(nowDays - 90),
                     TimeUnit.DAYS.toMillis(nowDays - 365)};

    Map<String, long[]> platformIncrements = new HashMap<>();
    Map<String, long[]> countryIncrements  = new HashMap<>();

    for (Account account : chunkAccounts) {

      Optional<Device> device = account.getMasterDevice();

      if (device.isPresent()) {

        long lastActiveMs = device.get().getLastSeen();

        String platform = null;

        if (device.get().getApnId() != null) {
          platform = PLATFORM_IOS;
        } else if (device.get().getGcmId() != null) {
          platform = PLATFORM_ANDROID;
        }

        if (platform != null) {
          String country = Util.getCountryCode(account.getNumber());

          long[] platformIncrement = getTallyFromMap(platformIncrements, platform);
          long[] countryIncrement  = getTallyFromMap(countryIncrements, country);

          for (int i = 0; i < agoMs.length; i++) {
            if (lastActiveMs > agoMs[i]) {
              platformIncrement[i]++;
              countryIncrement[i]++;
            }
          }
        }
      }
    }

    incrementTallies(fromNumber.orElse(UUID.randomUUID()), platformIncrements, countryIncrements);
  }

  private long[] getTallyFromMap(Map<String, long[]> map, String key) {
    long[] tally = map.get(key);
    if (tally == null) {
      tally = new long[INTERVALS.length];
      map.put(key, tally);
    }
    return tally;
  }

  private void incrementTallies(UUID fromUuid, Map<String, long[]> platformIncrements, Map<String, long[]> countryIncrements) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      String tallyValue = jedis.get(TALLY_KEY);
      ActiveUserTally activeUserTally;

      if (tallyValue == null) {
        activeUserTally = new ActiveUserTally(fromUuid, platformIncrements, countryIncrements);
      } else {
        activeUserTally = mapper.readValue(tallyValue, ActiveUserTally.class);

        if (!fromUuid.equals(activeUserTally.getFromUuid())) {
          activeUserTally.setFromUuid(fromUuid);
          Map<String, long[]> platformTallies = activeUserTally.getPlatforms();
          addTallyMaps(platformTallies, platformIncrements);
          Map<String, long[]> countryTallies = activeUserTally.getCountries();
          addTallyMaps(countryTallies, countryIncrements);
        }
      }

      jedis.set(TALLY_KEY, mapper.writeValueAsString(activeUserTally));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void addTallyMaps(Map<String, long[]> tallyMap, Map<String, long[]> incrementMap) {
    incrementMap.forEach((key, increments) -> {
      long[] tallies = tallyMap.get(key);
      if (tallies == null) {
        tallyMap.put(key, increments);
      } else {
        for (int i = 0; i < INTERVALS.length; i++) {
          tallies[i] += increments[i];
        }
      }
    });
  }

  private ActiveUserTally getFinalTallies() {
    try (Jedis jedis = jedisPool.getReadResource()) {
      return mapper.readValue(jedis.get(TALLY_KEY), ActiveUserTally.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String metricKey(String platform, String intervalName) {
    return MetricRegistry.name(ActiveUserCounter.class, intervalName + "_active_" + platform);
  }

  private String metricKey(String intervalName) {
    return MetricRegistry.name(ActiveUserCounter.class, intervalName + "_active");
  }

}
