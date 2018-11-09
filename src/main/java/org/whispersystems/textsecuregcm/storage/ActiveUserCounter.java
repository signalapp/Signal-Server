/**
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
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.Util;

import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class ActiveUserCounter implements Managed, Runnable {

  private static final long WORKER_TTL_MS       = 120_000L;
  private static final int  JITTER_BASE_MS      = 4_000; //25_000;
  private static final int  JITTER_VARIATION_MS = 2_000; //10_000;
  private static final int  CHUNK_SIZE          = 16_384;

  private static final Logger         logger         = LoggerFactory.getLogger(ActiveUserCounter.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer = metricRegistry.timer(name(ActiveUserCounter.class, "readChunk"));

  private static final String PLATFORM_IOS     = "ios";
  private static final String PLATFORM_ANDROID = "android";

  private static final String PLATFORMS[] = {PLATFORM_IOS, PLATFORM_ANDROID};
  private static final String INTERVALS[] = {"daily", "weekly", "monthly", "quarterly", "yearly"};


  private final Accounts                   accounts;
  private final ActiveUserCache            activeUserCache;
  private final String                     workerId;
  private final SecureRandom               random;

  private int              lastDate = ActiveUserCache.DEFAULT_DATE;
  private Optional<String> lastNumber;

  private boolean running;
  private boolean finished;

  public ActiveUserCounter(Accounts accounts, ActiveUserCache activeUserCache) {
    this.accounts        = accounts;
    this.activeUserCache = activeUserCache;
    this.random          = new SecureRandom();
    this.workerId        = UUID.randomUUID().toString();
  }

  @Override
  public synchronized void start() {
    running = true;
    new Thread(this).start();
  }

  @Override
  public synchronized void stop() {
    running = false;
    notifyAll();
    while (!finished) {
      Util.wait(this);
    }
  }

  @Override
  public void run() {

    while (sleepWhileRunning(getDelayWithJitter())) {
      int today = getDateOfToday();

      if (today > lastDate) {
        lastNumber = Optional.of(ActiveUserCache.INITIAL_NUMBER);
      }

      if (lastNumber.isPresent()) {
        try {
          doPeriodicWork(today);
        } catch (Throwable t) {
          logger.warn("error in active user count: ", t);
        }
      } else {
        logger.debug("run: no work available");
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  @VisibleForTesting
  public void doPeriodicWork(int today) {

    if (activeUserCache.claimActiveWorker(workerId, WORKER_TTL_MS)) {
      try {
        long startTimeMs = System.currentTimeMillis();
        Optional<String> number = activeUserCache.getNumber();
        int date = activeUserCache.getDate();

        if (today > date) {
          date = today;
          number = Optional.of(ActiveUserCache.INITIAL_NUMBER);
          activeUserCache.setNumber(number);
          activeUserCache.setDate(date);
          activeUserCache.resetTallies(PLATFORMS, INTERVALS);
          logger.info(date + " started");
        }

        if (number.isPresent()) {
          number = processChunk(date, number.get(), CHUNK_SIZE);
          activeUserCache.setNumber(number);
          if (!number.isPresent()) {
            registerMetrics();
            logger.info(date + " completed");
          }
        }

        lastDate = date;
        lastNumber = number;

        long endTimeMs = System.currentTimeMillis();
        long sleepInterval = getDelayWithJitter() - (endTimeMs - startTimeMs);
        if (sleepInterval > 0) sleepWhileRunning(sleepInterval);

      } finally {
        activeUserCache.releaseActiveWorker(workerId);
      }
    }
  }

  private void registerMetrics() {
    long intervalTallies[] = new long[INTERVALS.length];
    for (String platform : PLATFORMS) {
      long platformTallies[] = activeUserCache.getFinalTallies(platform, INTERVALS);
      for (int i = 0; i < INTERVALS.length; i++) {
        final long tally = platformTallies[i];
        logger.info(metricKey(platform, INTERVALS[i]) + " " + tally);
        metricRegistry.register(metricKey(platform, INTERVALS[i]),
                                new Gauge<Long>() {
                                  @Override
                                  public Long getValue() { return tally; }
                                });
        intervalTallies[i] += tally;
      }
    }

    for (int i = 0; i < INTERVALS.length; i++) {
      final long intervalTotal = intervalTallies[i];
      logger.info(metricKey(INTERVALS[i]) + " " + intervalTotal);
      metricRegistry.register(metricKey(INTERVALS[i]),
                              new Gauge<Long>() {
                                @Override
                                public Long getValue() { return intervalTotal; }
                              });
    }
  }

  private String metricKey(String platform, String interval) {
    return MetricRegistry.name(ActiveUserCounter.class, interval + "_active_" + platform);
  }

  private String metricKey(String interval) {
    return MetricRegistry.name(ActiveUserCounter.class, interval + "_active");
  }

  @VisibleForTesting
  public int getDateOfToday() {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    return Integer.valueOf(now.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
  }

  private synchronized boolean sleepWhileRunning(long delayMs) {
    if (running) Util.wait(this, delayMs);
    return running;
  }

  private long getDelayWithJitter() {
    return (long) (JITTER_BASE_MS + random.nextDouble() * JITTER_VARIATION_MS);
  }

  private Optional<String> processChunk(int date, String number, int count) {
    logger.debug("processChunk date=" + date + " number=" + number + " count=" + count);
    String lastNumber = null;
    long nowDays  = TimeUnit.MILLISECONDS.toDays(getDateMidnightMs(date));
    long agoMs[]  = {TimeUnit.DAYS.toMillis(nowDays - 1),
                     TimeUnit.DAYS.toMillis(nowDays - 7),
                     TimeUnit.DAYS.toMillis(nowDays - 30),
                     TimeUnit.DAYS.toMillis(nowDays - 90),
                     TimeUnit.DAYS.toMillis(nowDays - 365)};
    long ios[]     = {0, 0, 0, 0, 0};
    long android[] = {0, 0, 0, 0, 0};

    List<Account> chunkAccounts = readChunk(number, count);
    for (Account account : chunkAccounts) {
      lastNumber = account.getNumber();

      Optional<Device> device = account.getMasterDevice();

      if (!device.isPresent()) continue;

      long lastActiveMs = device.get().getLastSeen();

      if (device.get().getApnId() != null) {
        for (int i = 0; i < agoMs.length; i++)
          if (lastActiveMs > agoMs[i]) ios[i]++;
      } else if (device.get().getGcmId() != null) {
        for (int i = 0; i < agoMs.length; i++)
          if (lastActiveMs > agoMs[i]) android[i]++;
      }
    }
    activeUserCache.incrementTallies(PLATFORM_IOS, INTERVALS, ios);
    activeUserCache.incrementTallies(PLATFORM_ANDROID, INTERVALS, android);
    return Optional.fromNullable(lastNumber);
  }

  private List<Account> readChunk(String number, int count) {
    try (Timer.Context timer = readChunkTimer.time()) {
      return accounts.getAllFrom(number, count);
    }
  }

  private long getDateMidnightMs(int date) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddz");
    try {
      return format.parse(date + "UTC").getTime();
    } catch (ParseException e) {
      throw new AssertionError("unexpected: " + date);
    }
  }

}
