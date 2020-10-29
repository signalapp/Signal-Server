/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import org.whispersystems.textsecuregcm.entities.ActiveUserTally;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ActiveUserCounter extends AccountDatabaseCrawlerListener {

  private static final String TALLY_KEY         = "active_user_tally";

  private static final String PLATFORM_IOS     = "ios";
  private static final String PLATFORM_ANDROID = "android";

  private static final String INTERVALS[] = {"daily", "weekly", "monthly", "quarterly", "yearly"};

  private final MetricsFactory            metricsFactory;
  private final FaultTolerantRedisCluster cacheCluster;
  private final ObjectMapper              mapper;

  public ActiveUserCounter(MetricsFactory metricsFactory, FaultTolerantRedisCluster cacheCluster) {
    this.metricsFactory         = metricsFactory;
    this.cacheCluster           = cacheCluster;
    this.mapper                 = SystemMapper.getMapper();
  }

  @Override
  public void onCrawlStart() {
    cacheCluster.useCluster(connection -> connection.sync().del(TALLY_KEY));
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
      try (final ScheduledReporter reporter = reporterFactory.build(metrics)) {
        reporter.report();
      }
    }
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromNumber, List<Account> chunkAccounts) {
    long nowHours  = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis());
    long agoMs[]  = {TimeUnit.HOURS.toMillis(nowHours - 1   * 24 - 8),
                     TimeUnit.HOURS.toMillis(nowHours - 7   * 24),
                     TimeUnit.HOURS.toMillis(nowHours - 30  * 24),
                     TimeUnit.HOURS.toMillis(nowHours - 90  * 24),
                     TimeUnit.HOURS.toMillis(nowHours - 365 * 24)};

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
    try {
      final String tallyValue = cacheCluster.withCluster(connection -> connection.sync().get(TALLY_KEY));

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

      final String tallyJson = mapper.writeValueAsString(activeUserTally);

      cacheCluster.useCluster(connection -> connection.sync().set(TALLY_KEY, tallyJson));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
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
    try {
      final String tallyJson = cacheCluster.withCluster(connection -> connection.sync().get(TALLY_KEY));

      return mapper.readValue(tallyJson, ActiveUserTally.class);
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
