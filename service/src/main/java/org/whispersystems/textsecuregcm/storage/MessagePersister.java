/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;
import static io.micrometer.core.instrument.Metrics.counter;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.dynamodb.model.ItemCollectionSizeLimitExceededException;

public class MessagePersister implements Managed {

  private final MessagesCache messagesCache;
  private final MessagesManager messagesManager;
  private final AccountsManager accountsManager;
  private final ClientPresenceManager clientPresenceManager;
  private final KeysManager keysManager;

  private final Duration persistDelay;

  private final boolean dedicatedProcess;
  private final Thread[] workerThreads;
  private volatile boolean running;

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer getQueuesTimer = metricRegistry.timer(name(MessagePersister.class, "getQueues"));
  private final Timer persistQueueTimer = metricRegistry.timer(name(MessagePersister.class, "persistQueue"));
  private final Meter persistQueueExceptionMeter = metricRegistry.meter(
      name(MessagePersister.class, "persistQueueException"));
  private final Counter oversizedQueueCounter = counter(name(MessagePersister.class, "persistQueueOversized"));
  private final Histogram queueCountHistogram = metricRegistry.histogram(name(MessagePersister.class, "queueCount"));
  private final Histogram queueSizeHistogram = metricRegistry.histogram(name(MessagePersister.class, "queueSize"));
  private final ExecutorService executor;

  static final int QUEUE_BATCH_LIMIT = 100;
  static final int MESSAGE_BATCH_LIMIT = 100;

  private static final long EXCEPTION_PAUSE_MILLIS = Duration.ofSeconds(3).toMillis();
  public static final Duration UNLINK_TIMEOUT = Duration.ofHours(1);

  private static final int CONSECUTIVE_EMPTY_CACHE_REMOVAL_LIMIT = 3;

  private static final Logger logger = LoggerFactory.getLogger(MessagePersister.class);

  public MessagePersister(final MessagesCache messagesCache, final MessagesManager messagesManager,
      final AccountsManager accountsManager, final ClientPresenceManager clientPresenceManager,
      final KeysManager keysManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final Duration persistDelay,
      final int dedicatedProcessWorkerThreadCount,
      final ExecutorService executor
  ) {
    this.messagesCache = messagesCache;
    this.messagesManager = messagesManager;
    this.accountsManager = accountsManager;
    this.clientPresenceManager = clientPresenceManager;
    this.keysManager = keysManager;
    this.persistDelay = persistDelay;
    this.workerThreads = new Thread[dedicatedProcessWorkerThreadCount];
    this.dedicatedProcess = true;
    this.executor = executor;

    for (int i = 0; i < workerThreads.length; i++) {
      workerThreads[i] = new Thread(() -> {
        while (running) {
          if (dynamicConfigurationManager.getConfiguration().getMessagePersisterConfiguration()
              .isPersistenceEnabled()) {
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
        final byte deviceId = MessagesCache.getDeviceIdFromQueueName(queue);

        final Optional<Account> maybeAccount = accountsManager.getByAccountIdentifier(accountUuid);
        if (maybeAccount.isEmpty()) {
          logger.error("No account record found for account {}", accountUuid);
          continue;
        }
        try {
          persistQueue(maybeAccount.get(), deviceId);
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
  void persistQueue(final Account account, final byte deviceId) throws MessagePersistenceException {
    final UUID accountUuid = account.getUuid();
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
      } catch (ItemCollectionSizeLimitExceededException e) {
        oversizedQueueCounter.increment();
        unlinkLeastActiveDevice(account, deviceId); // this will either do a deferred reschedule for retry or throw
      } finally {
        messagesCache.unlockQueueForPersistence(accountUuid, deviceId);
      }
    }
  }

  @VisibleForTesting
  void unlinkLeastActiveDevice(final Account account, byte destinationDeviceId) throws MessagePersistenceException {
    if (!messagesCache.lockAccountForMessagePersisterCleanup(account.getUuid())) {
      // don't try to unlink an account multiple times in parallel; just fail this persist attempt
      // and we'll try again, hopefully deletions in progress will have made room
      throw new MessagePersistenceException("account has a full queue and another device-unlinking attempt is in progress");
    }

    // Selection logic:

    // The primary device is never unlinked
    // The least-recently-seen inactive device gets unlinked
    // If there are none, the device with the oldest queued message (not necessarily the
    // least-recently-seen; a device could be connecting frequently but have some problem fetching
    // its messages) is unlinked
    final Device deviceToDelete = account.getDevices()
        .stream()
        .filter(d -> !d.isPrimary() && !d.isEnabled())
        .min(Comparator.comparing(Device::getLastSeen))
        .or(() ->
            Flux.fromIterable(account.getDevices())
            .filter(d -> !d.isPrimary())
            .flatMap(d ->
                messagesManager
                .getEarliestUndeliveredTimestampForDevice(account.getUuid(), d.getId())
                .map(t -> Tuples.of(d, t)))
            .sort(Comparator.comparing(Tuple2::getT2))
            .map(Tuple2::getT1)
            .next()
            .blockOptional())
        .orElseThrow(() -> new MessagePersistenceException("account has a full queue and no unlinkable devices"));

    logger.warn("Failed to persist queue {}::{} due to overfull queue; will unlink device {}{}",
        account.getUuid(), destinationDeviceId, deviceToDelete.getId(), deviceToDelete.getId() == destinationDeviceId ? "" : " and schedule for retry");
    executor.execute(
        () -> {
          try {
            // Lock the device's auth token first to prevent it from connecting while we're
            // destroying its queue, but we don't want to completely remove the device from the
            // account until we're sure the messages have been cleared because otherwise we won't
            // be able to find it later to try again, in the event of a failure or timeout
            final Account updatedAccount = accountsManager.update(
                account,
                a -> a.getDevice(deviceToDelete.getId()).ifPresent(Device::lockAuthTokenHash));
            clientPresenceManager.disconnectPresence(account.getUuid(), deviceToDelete.getId());
            CompletableFuture
                .allOf(
                    keysManager.deleteSingleUsePreKeys(account.getUuid(), deviceToDelete.getId()),
                    messagesManager.clear(account.getUuid(), deviceToDelete.getId()))
                .orTimeout((UNLINK_TIMEOUT.toSeconds() * 3) / 4, TimeUnit.SECONDS)
                .join();
            accountsManager.removeDevice(updatedAccount, deviceToDelete.getId()).join();
          } finally {
            messagesCache.unlockAccountForMessagePersisterCleanup(account.getUuid());
            if (deviceToDelete.getId() != destinationDeviceId) {              // no point in persisting a device we just purged
              messagesCache.addQueueToPersist(account.getUuid(), destinationDeviceId);
            }
          }
        });
  }
}
