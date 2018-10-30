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
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class ActiveUserCounter implements Managed, Runnable {

  private static final long WORKER_TTL_MS       = 3600_000L;
  private static final int  JITTER_BASE_MS      = 45_000;
  private static final int  JITTER_VARIATION_MS = 30_000;
  private static final int  CHUNK_SIZE          = 10_000;

  private static final Logger         logger         = LoggerFactory.getLogger(ActiveUserCounter.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer = metricRegistry.timer(name(ActiveUserCounter.class, "readChunk"));

  private final Accounts        accounts;
  private final ActiveUserCache activeUserCache;
  private final String          workerId;
  private final SecureRandom    random;
  
  private int lastDate   = ActiveUserCache.DEFAULT_DATE;
  private int lastCount  = ActiveUserCache.DEFAULT_COUNT;
  private int lastOffset = ActiveUserCache.DEFAULT_OFFSET;
    
  private boolean running;
  private boolean finished;

  public ActiveUserCounter(ActiveUserCache activeUserCache, Accounts accounts) {
    this.accounts        = accounts;
    this.activeUserCache = activeUserCache;
    this.random          = new SecureRandom();
    this.workerId        = generateWorkerId(random);
  }

  private static String generateWorkerId(SecureRandom random) {
    byte[] workerIdBytes = new byte[16];
    random.nextBytes(workerIdBytes);
    return Hex.toString(workerIdBytes);
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
        lastCount = ActiveUserCache.DEFAULT_COUNT;
        lastOffset = ActiveUserCache.DEFAULT_OFFSET;
      }

      if (lastOffset < lastCount) {
        try {
          doPeriodicWork(today);
        } catch (Throwable t) {
          logger.warn("error in active user count: ", t);
        }
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
        int offset = activeUserCache.getOffset();
        int count = activeUserCache.getCount();
        int date = activeUserCache.getDate();

        if (today > date) {
          date = today;
          offset = 0;
          count = accounts.getRecordCount();
          activeUserCache.setDate(date);
          activeUserCache.setOffset(offset);
          activeUserCache.setCount(count);
          activeUserCache.resetTallies();
        }
                    
        if (offset < count) {
          processChunk(date, offset, CHUNK_SIZE);
          offset += CHUNK_SIZE;
          activeUserCache.setOffset(offset);
        }

        lastDate = date;
        lastCount = count;
        lastOffset = offset;

        if (lastOffset >= lastCount)
          activeUserCache.registerTallies();

      } finally {
        activeUserCache.releaseActiveWorker(workerId);
      }
    }
  }

  private int getDateOfToday() {
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

  private void processChunk(int date, long offset, int count) {
    long nowDays  = TimeUnit.MILLISECONDS.toDays(getDateMidnightMs(date));
    long agoMs[]  = {TimeUnit.DAYS.toMillis(nowDays - 1),
                     TimeUnit.DAYS.toMillis(nowDays - 7),
                     TimeUnit.DAYS.toMillis(nowDays - 30),
                     TimeUnit.DAYS.toMillis(nowDays - 90),
                     TimeUnit.DAYS.toMillis(nowDays - 365)};
    long ios[]     = {0, 0, 0, 0, 0};
    long android[] = {0, 0, 0, 0, 0};
        
    List<Accounts.ActiveUser> chunkAccounts = readChunk(offset, count);
    for (Accounts.ActiveUser user : chunkAccounts) {
      String platform = user.getPlatform();
      long lastActiveMs = user.getLastActiveMs();
      switch (platform) {
      case "i":
        for (int i = 0; i < agoMs.length; i++)
          if (lastActiveMs > agoMs[i]) ios[i]++;
        break;
      case "a":
        for (int i = 0; i < agoMs.length; i++)
          if (lastActiveMs > agoMs[i]) android[i]++;
        break;
      default:
        throw new AssertionError("unexpected: " + platform);
      }
    }
    activeUserCache.incrementTallies(ios, android);
  }

  private List<Accounts.ActiveUser> readChunk(long offset, int count) {
    try (Timer.Context timer = readChunkTimer.time()) {
      Optional<List<Accounts.ActiveUser>> activeUsers;
      activeUsers = Optional.fromNullable(accounts.getActiveUsers(offset, count));
      return activeUsers.or(Collections::emptyList);
    }
  }

  private long getDateMidnightMs(int date) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddz");
    try {
      return format.parse(date + "UTC").getTime();
    } catch (Exception e) {
      throw new AssertionError("unexpected: " + date);
    }
  }

}
