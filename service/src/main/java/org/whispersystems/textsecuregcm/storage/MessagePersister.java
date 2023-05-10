/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessagePersisterConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

public class MessagePersister implements Managed {

  private final MessagesCache messagesCache;
  private final MessagesManager messagesManager;
  private final AccountsManager accountsManager;

  private final Duration persistDelay;

  private final boolean dedicatedProcess;
  private final Thread[] workerThreads;
  private volatile boolean running;

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer getQueuesTimer = metricRegistry.timer(name(MessagePersister.class, "getQueues"));
  private final Timer persistQueueTimer = metricRegistry.timer(name(MessagePersister.class, "persistQueue"));
  private final Meter persistQueueExceptionMeter = metricRegistry.meter(
      name(MessagePersister.class, "persistQueueException"));
  private final Histogram queueCountHistogram = metricRegistry.histogram(name(MessagePersister.class, "queueCount"));
  private final Histogram queueSizeHistogram = metricRegistry.histogram(name(MessagePersister.class, "queueSize"));

  static final int QUEUE_BATCH_LIMIT = 100;
  static final int MESSAGE_BATCH_LIMIT = 100;

  private static final long EXCEPTION_PAUSE_MILLIS = Duration.ofSeconds(3).toMillis();

  private static final int DEFAULT_WORKER_THREAD_COUNT = 4;

  private static final int CONSECUTIVE_EMPTY_CACHE_REMOVAL_LIMIT = 3;

  private static final Logger logger = LoggerFactory.getLogger(MessagePersister.class);

  public MessagePersister(final MessagesCache messagesCache, final MessagesManager messagesManager,
      final AccountsManager accountsManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final Duration persistDelay,
      final Optional<Integer> dedicatedProcessWorkerThreadCount) {
    this.messagesCache = messagesCache;
    this.messagesManager = messagesManager;
    this.accountsManager = accountsManager;
    this.persistDelay = persistDelay;
    this.workerThreads = dedicatedProcessWorkerThreadCount.map(Thread[]::new)
        .orElseGet(() -> new Thread[DEFAULT_WORKER_THREAD_COUNT]);
    this.dedicatedProcess = dedicatedProcessWorkerThreadCount.isPresent();

    for (int i = 0; i < workerThreads.length; i++) {
      workerThreads[i] = new Thread(() -> {
        while (running) {
          if (enabled(dynamicConfigurationManager)) {
            try {
              final int queuesPersisted = persistNextQueues(Instant.now());
              queueCountHistogram.update(queuesPersisted);

              if (queuesPersisted == 0) {
                Util.sleep(100);
              }
            } catch (final Throwable t) {
              logger.warn("Failed to persist queues", t);
              Util.sleep(EXCEPTION_PAUSE_MILLIS);
            }
          } else {
            Util.sleep(1000);
          }
        }
      }, "MessagePersisterWorker-" + i);
    }
  }

  @VisibleForTesting
  boolean enabled(final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    final DynamicMessagePersisterConfiguration messagePersisterConfiguration = dynamicConfigurationManager.getConfiguration()
        .getMessagePersisterConfiguration();
    if (dedicatedProcess) {
      return messagePersisterConfiguration.isDedicatedProcessEnabled();
    }

    return messagePersisterConfiguration.isServerPersistenceEnabled();
  }

  @VisibleForTesting
  Duration getPersistDelay() {
    return persistDelay;
  }

  @Override
  public void start() {
    running = true;

    for (final Thread workerThread : workerThreads) {
      workerThread.start();
    }
  }

  @Override
  public void stop() {
    running = false;

    for (final Thread workerThread : workerThreads) {
      try {
        workerThread.join();
      } catch (final InterruptedException e) {
        logger.warn("Interrupted while waiting for worker thread to complete current operation");
      }
    }
  }

  @VisibleForTesting
  int persistNextQueues(final Instant currentTime) {
    final int slot = messagesCache.getNextSlotToPersist();

    List<String> queuesToPersist;
    int queuesPersisted = 0;

    do {
      try (final Timer.Context ignored = getQueuesTimer.time()) {
        queuesToPersist = messagesCache.getQueuesToPersist(slot, currentTime.minus(persistDelay), QUEUE_BATCH_LIMIT);
      }

      for (final String queue : queuesToPersist) {
        final UUID accountUuid = MessagesCache.getAccountUuidFromQueueName(queue);
        final long deviceId = MessagesCache.getDeviceIdFromQueueName(queue);

        try {
          persistQueue(accountUuid, deviceId);
        } catch (final Exception e) {
          persistQueueExceptionMeter.mark();
          logger.warn("Failed to persist queue {}::{}; will schedule for retry", accountUuid, deviceId, e);

          messagesCache.addQueueToPersist(accountUuid, deviceId);

          Util.sleep(EXCEPTION_PAUSE_MILLIS);
        }
      }

      queuesPersisted += queuesToPersist.size();
    } while (queuesToPersist.size() >= QUEUE_BATCH_LIMIT);

    return queuesPersisted;
  }

  @VisibleForTesting
  void persistQueue(final UUID accountUuid, final long deviceId) throws MessagePersistenceException {
    final Optional<Account> maybeAccount = accountsManager.getByAccountIdentifier(accountUuid);

    if (maybeAccount.isEmpty()) {
      logger.error("No account record found for account {}", accountUuid);
      return;
    }

    try (final Timer.Context ignored = persistQueueTimer.time()) {
      messagesCache.lockQueueForPersistence(accountUuid, deviceId);

      try {
        int messageCount = 0;
        List<MessageProtos.Envelope> messages;

        int consecutiveEmptyCacheRemovals = 0;

        do {
          messages = messagesCache.getMessagesToPersist(accountUuid, deviceId, MESSAGE_BATCH_LIMIT);

          int messagesRemovedFromCache = messagesManager.persistMessages(accountUuid, deviceId, messages);
          messageCount += messages.size();

          if (messagesRemovedFromCache == 0) {
            consecutiveEmptyCacheRemovals += 1;
          } else {
            consecutiveEmptyCacheRemovals = 0;
          }

          if (consecutiveEmptyCacheRemovals > CONSECUTIVE_EMPTY_CACHE_REMOVAL_LIMIT) {
            throw new MessagePersistenceException("persistence failure loop detected");
          }

        } while (!messages.isEmpty());

        queueSizeHistogram.update(messageCount);
      } finally {
        messagesCache.unlockQueueForPersistence(accountUuid, deviceId);
      }
    }
  }
}
