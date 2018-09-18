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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.storage.DirectoryManager.BatchOperationHandle;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.Util;

import javax.ws.rs.ProcessingException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "readChunk"));
  private static final Timer          sendChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendChunk"));
  private static final Meter          sendChunkErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendChunkError"));

  private static final long   WORKER_TTL_MS              = 120_000L;
  private static final long   PERIOD                     = 86400_000L;
  private static final long   MAXIMUM_CHUNK_INTERVAL     = 30_000L;
  private static final long   DEFAULT_CHUNK_INTERVAL     = 10_000L;
  private static final long   MINIMUM_CHUNK_INTERVAL     = 500L;
  private static final long   ACCELERATED_CHUNK_INTERVAL = 10L;
  private static final int    CHUNK_SIZE                 = 1000;
  private static final double JITTER_MAX                 = 0.20;

  private final Accounts                      readOnlyAccounts;
  private final DirectoryManager              directoryManager;
  private final DirectoryReconciliationClient reconciliationClient;
  private final DirectoryReconciliationCache  reconciliationCache;
  private final String                        workerId;
  private final SecureRandom                  random;

  private boolean running;
  private boolean finished;

  public DirectoryReconciler(DirectoryReconciliationClient reconciliationClient,
                             DirectoryReconciliationCache reconciliationCache,
                             DirectoryManager directoryManager,
                             Accounts readOnlyAccounts) {
    this.readOnlyAccounts     = readOnlyAccounts;
    this.directoryManager     = directoryManager;
    this.reconciliationClient = reconciliationClient;
    this.reconciliationCache  = reconciliationCache;
    this.random               = new SecureRandom();
    this.workerId             = generateWorkerId(random);
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
    long delayMs = DEFAULT_CHUNK_INTERVAL;

    while (sleepWhileRunning(getDelayWithJitter(delayMs))) {
      try {
        delayMs = DEFAULT_CHUNK_INTERVAL;
        delayMs = getBoundedChunkInterval(PERIOD * CHUNK_SIZE / getAccountCount());
        delayMs = doPeriodicWork(delayMs);
      } catch (Throwable t) {
        logger.warn("error in directory reconciliation: ", t);
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  @VisibleForTesting
  public long doPeriodicWork(long intervalMs) {
    long nextIntervalTimeMs = System.currentTimeMillis() + intervalMs;

    if (reconciliationCache.claimActiveWork(workerId, WORKER_TTL_MS)) {
      if (processChunk()) {
        if (!reconciliationCache.isAccelerated()) {
          long timeUntilNextIntervalMs = getTimeUntilNextInterval(nextIntervalTimeMs);
          reconciliationCache.claimActiveWork(workerId, timeUntilNextIntervalMs);
          return timeUntilNextIntervalMs;
        } else {
          return ACCELERATED_CHUNK_INTERVAL;
        }
      }
    }
    return intervalMs;
  }

  @VisibleForTesting
  public long getAccountCount() {
    Optional<Long> cachedCount = reconciliationCache.getCachedAccountCount();

    if (cachedCount.isPresent()) {
      return cachedCount.get();
    }

    long count = readOnlyAccounts.getCount();
    reconciliationCache.setCachedAccountCount(count);
    return count;
  }

  private synchronized boolean sleepWhileRunning(long delayMs) {
    long startTimeMs = System.currentTimeMillis();
    while (running && delayMs > 0) {
      Util.wait(this, delayMs);

      long nowMs = System.currentTimeMillis();
      delayMs -= Math.abs(nowMs - startTimeMs);
    }
    return running;
  }

  private long getTimeUntilNextInterval(long nextIntervalTimeMs) {
    long nextIntervalMs = nextIntervalTimeMs - System.currentTimeMillis();
    return getBoundedChunkInterval(nextIntervalMs);
  }

  private long getBoundedChunkInterval(long intervalMs) {
    return Math.max(Math.min(intervalMs, MAXIMUM_CHUNK_INTERVAL), MINIMUM_CHUNK_INTERVAL);
  }

  private long getDelayWithJitter(long delayMs) {
    long randomJitterMs = (long) (random.nextDouble() * JITTER_MAX * delayMs);
    return delayMs + randomJitterMs;
  }

  private boolean processChunk() {
    Optional<String> fromNumber    = reconciliationCache.getLastNumber();
    List<Account>    chunkAccounts = readChunk(fromNumber, CHUNK_SIZE);

    writeChunktoDirectoryCache(chunkAccounts);

    DirectoryReconciliationRequest  request           = createChunkRequest(fromNumber, chunkAccounts);
    DirectoryReconciliationResponse sendChunkResponse = sendChunk(request);

    if (sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.MISSING ||
        request.getToNumber() == null) {
      reconciliationCache.clearAccelerate();
    }

    if (sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.OK) {
      reconciliationCache.setLastNumber(Optional.fromNullable(request.getToNumber()));
    } else if (sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.MISSING) {
      reconciliationCache.setLastNumber(Optional.absent());
    }

    return sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.OK;
  }

  private List<Account> readChunk(Optional<String> fromNumber, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      Optional<List<Account>> chunkAccounts;

      if (fromNumber.isPresent()) {
        chunkAccounts = Optional.fromNullable(readOnlyAccounts.getAllFrom(fromNumber.get(), chunkSize));
      } else {
        chunkAccounts = Optional.fromNullable(readOnlyAccounts.getAllFrom(chunkSize));
      }

      return chunkAccounts.or(Collections::emptyList);
    }
  }

  private void writeChunktoDirectoryCache(List<Account> accounts) {
    if (accounts.isEmpty()) {
      return;
    }

    BatchOperationHandle batchOperation = directoryManager.startBatchOperation();
    try {
      for (Account account : accounts) {
        if (account.isActive()) {
          byte[]        token         = Util.getContactToken(account.getNumber());
          ClientContact clientContact = new ClientContact(token, null, account.isVoiceSupported(), account.isVideoSupported());

          directoryManager.add(batchOperation, clientContact);
        } else {
          directoryManager.remove(batchOperation, account.getNumber());
        }
      }
    } finally {
      directoryManager.stopBatchOperation(batchOperation);
    }
  }

  private DirectoryReconciliationRequest createChunkRequest(Optional<String> fromNumber, List<Account> accounts) {
    List<String> numbers = accounts.stream()
                                   .filter(Account::isActive)
                                   .map(Account::getNumber)
                                   .collect(Collectors.toList());

    Optional<String> toNumber = Optional.absent();
    if (!accounts.isEmpty()) {
      toNumber = Optional.of(accounts.get(accounts.size() - 1).getNumber());
    }

    return new DirectoryReconciliationRequest(fromNumber.orNull(), toNumber.orNull(), numbers);
  }

  private DirectoryReconciliationResponse sendChunk(DirectoryReconciliationRequest request) {
    try (Timer.Context timer = sendChunkTimer.time()) {
      DirectoryReconciliationResponse response = reconciliationClient.sendChunk(request);
      if (response.getStatus() != DirectoryReconciliationResponse.Status.OK) {
        sendChunkErrorMeter.mark();
        logger.warn("reconciliation error: " + response.getStatus());
      }
      return response;
    } catch (ProcessingException ex) {
      sendChunkErrorMeter.mark();
      logger.warn("request error: ", ex);
      throw new ProcessingException(ex);
    }
  }

}
