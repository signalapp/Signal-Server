/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.dynamodb.model.ItemCollectionSizeLimitExceededException;

public class MessagePersister implements Managed {

  private final MessagesCache messagesCache;
  private final MessagesManager messagesManager;
  private final AccountsManager accountsManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final Duration persistDelay;

  private final Thread[] workerThreads;
  private volatile boolean running;

  private static final String OVERSIZED_QUEUE_COUNTER_NAME = name(MessagePersister.class, "persistQueueOversized");

  private final Timer getQueuesTimer = Metrics.timer(name(MessagePersister.class, "getQueues"));
  private final Timer persistQueueTimer = Metrics.timer(name(MessagePersister.class, "persistQueue"));
  private final Counter persistQueueExceptionMeter = Metrics.counter(
      name(MessagePersister.class, "persistQueueException"));
  private static final Counter trimmedMessageCounter = Metrics.counter(name(MessagePersister.class, "trimmedMessage"));
  private static final Counter trimmedMessageBytesCounter = Metrics.counter(name(MessagePersister.class, "trimmedMessageBytes"));
  private final DistributionSummary queueCountDistributionSummery = DistributionSummary.builder(
          name(MessagePersister.class, "queueCount"))
      .publishPercentiles(0.5, 0.75, 0.95, 0.99, 0.999)
      .distributionStatisticExpiry(Duration.ofMinutes(10))
      .register(Metrics.globalRegistry);
  private final DistributionSummary queueSizeDistributionSummery = DistributionSummary.builder(
          name(MessagePersister.class, "queueSize"))
      .publishPercentiles(0.5, 0.75, 0.95, 0.99, 0.999)
      .distributionStatisticExpiry(Duration.ofMinutes(10))
      .register(Metrics.globalRegistry);

  static final int QUEUE_BATCH_LIMIT = 100;
  static final int MESSAGE_BATCH_LIMIT = 100;
  static final int CACHE_PAGE_SIZE = 100;

  private static final long EXCEPTION_PAUSE_MILLIS = Duration.ofSeconds(3).toMillis();

  private static final int CONSECUTIVE_EMPTY_CACHE_REMOVAL_LIMIT = 3;

  private static final Logger logger = LoggerFactory.getLogger(MessagePersister.class);

  public MessagePersister(final MessagesCache messagesCache,
      final MessagesManager messagesManager,
      final AccountsManager accountsManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final Duration persistDelay,
      final int dedicatedProcessWorkerThreadCount) {

    this.messagesCache = messagesCache;
    this.messagesManager = messagesManager;
    this.accountsManager = accountsManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.persistDelay = persistDelay;
    this.workerThreads = new Thread[dedicatedProcessWorkerThreadCount];

    for (int i = 0; i < workerThreads.length; i++) {
      workerThreads[i] = new Thread(() -> {
        while (running) {
          if (dynamicConfigurationManager.getConfiguration().getMessagePersisterConfiguration()
              .isPersistenceEnabled()) {
            try {
              final int queuesPersisted = persistNextQueues(Instant.now());
              queueCountDistributionSummery.record(queuesPersisted);

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
      queuesToPersist = getQueuesTimer.record(
          () -> messagesCache.getQueuesToPersist(slot, currentTime.minus(persistDelay), QUEUE_BATCH_LIMIT));

      for (final String queue : queuesToPersist) {
        final UUID accountUuid = MessagesCache.getAccountUuidFromQueueName(queue);
        final byte deviceId = MessagesCache.getDeviceIdFromQueueName(queue);

        final Optional<Account> maybeAccount = accountsManager.getByAccountIdentifier(accountUuid);
        if (maybeAccount.isEmpty()) {
          logger.error("No account record found for account {}", accountUuid);
          continue;
        }
        final Optional<Device> maybeDevice = maybeAccount.flatMap(account -> account.getDevice(deviceId));
        if (maybeDevice.isEmpty()) {
          logger.error("Account {} does not have a device with id {}", accountUuid, deviceId);
          continue;
        }
        try {
          persistQueue(maybeAccount.get(), maybeDevice.get());
        } catch (final Exception e) {
          persistQueueExceptionMeter.increment();
          logger.warn("Failed to persist queue {}::{}; will schedule for retry", accountUuid, deviceId, e);

          messagesCache.addQueueToPersist(accountUuid, deviceId);

          if (!(e instanceof MessagePersistenceException)) {
            // Pause after unexpected exceptions
            Util.sleep(EXCEPTION_PAUSE_MILLIS);
          }
        }
      }

      queuesPersisted += queuesToPersist.size();
    } while (queuesToPersist.size() >= QUEUE_BATCH_LIMIT);

    return queuesPersisted;
  }

  @VisibleForTesting
  void persistQueue(final Account account, final Device device) throws MessagePersistenceException {
    final UUID accountUuid = account.getUuid();
    final byte deviceId = device.getId();

    final Timer.Sample sample = Timer.start();

    messagesCache.lockQueueForPersistence(accountUuid, deviceId);

    try {
      int messageCount = 0;
      List<MessageProtos.Envelope> messages;

      int consecutiveEmptyCacheRemovals = 0;

      do {
        messages = messagesCache.getMessagesToPersist(accountUuid, deviceId, MESSAGE_BATCH_LIMIT);

        int messagesRemovedFromCache = messagesManager.persistMessages(accountUuid, device, messages);
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

      queueSizeDistributionSummery.record(messageCount);
    } catch (ItemCollectionSizeLimitExceededException e) {
      final boolean isPrimary = deviceId == Device.PRIMARY_ID;
      Metrics.counter(OVERSIZED_QUEUE_COUNTER_NAME, "primary", String.valueOf(isPrimary)).increment();
      // may throw, in which case we'll retry later by the usual mechanism
      if (isPrimary) {
        logger.warn("Failed to persist queue {}::{} due to overfull queue; will trim oldest messages",
            account.getUuid(), deviceId);
        trimQueue(account, deviceId);
        throw new MessagePersistenceException("Could not persist due to an overfull queue. Trimmed primary queue, a subsequent retry may succeed");
      } else {
        logger.warn("Failed to persist queue {}::{} due to overfull queue; will unlink device", account.getUuid(),
            deviceId);
        accountsManager.removeDevice(account, deviceId).join();
      }
    } finally {
      messagesCache.unlockQueueForPersistence(accountUuid, deviceId);
      sample.stop(persistQueueTimer);
    }

  }

  private void trimQueue(final Account account, byte deviceId) {
    final UUID aci = account.getIdentifier(IdentityType.ACI);

    final Optional<Device> maybeDevice = account.getDevice(deviceId);
    if (maybeDevice.isEmpty()) {
      logger.warn("Not deleting messages for overfull queue {}::{}, deviceId {} does not exist",
          aci, deviceId, deviceId);
      return;
    }
    final Device device = maybeDevice.get();

    // Calculate how many bytes we should trim
    final long cachedMessageBytes = Flux
        .from(messagesCache.getMessagesToPersistReactive(aci, deviceId, CACHE_PAGE_SIZE))
        .reduce(0, (acc, envelope) -> acc + envelope.getSerializedSize())
        .block();
    final double extraRoomRatio = this.dynamicConfigurationManager.getConfiguration()
        .getMessagePersisterConfiguration()
        .getTrimOversizedQueueExtraRoomRatio();
    final long targetDeleteBytes = Math.round(cachedMessageBytes * extraRoomRatio);

    final AtomicLong oldestMessage = new AtomicLong(0L);
    final AtomicLong newestMessage = new AtomicLong(0L);
    final AtomicLong bytesDeleted = new AtomicLong(0L);

    // Iterate from the oldest message until we've removed targetDeleteBytes
    final Pair<Long, Long> outcomes = Flux.from(messagesManager.getMessagesForDeviceReactive(aci, device, false))
        .concatMap(envelope -> {
          if (bytesDeleted.getAndAdd(envelope.getSerializedSize()) >= targetDeleteBytes) {
            return Mono.just(Optional.<MessageProtos.Envelope>empty());
          }
          oldestMessage.compareAndSet(0L, envelope.getServerTimestamp());
          newestMessage.set(envelope.getServerTimestamp());
          return Mono.just(Optional.of(envelope));
        })
        .takeWhile(Optional::isPresent)
        .flatMap(maybeEnvelope -> {
          final MessageProtos.Envelope envelope = maybeEnvelope.get();
          trimmedMessageCounter.increment();
          trimmedMessageBytesCounter.increment(envelope.getSerializedSize());
          return Mono
              .fromCompletionStage(() -> messagesManager
                  .delete(aci, device, UUID.fromString(envelope.getServerGuid()), envelope.getServerTimestamp()))
              .retryWhen(Retry.backoff(5, Duration.ofSeconds(1)))
              .map(Optional::isPresent);
        })
        .reduce(Pair.of(0L, 0L), (acc, deleted) -> deleted
            ? Pair.of(acc.getLeft() + 1, acc.getRight())
            : Pair.of(acc.getLeft(), acc.getRight() + 1))
        .block();

    logger.warn(
        "Finished trimming {}:{}. Oldest message = {}, newest message = {}. Attempted to delete {} persisted bytes to make room for {} cached message bytes.  Delete outcomes: {} present, {} missing.",
        aci, deviceId,
        Instant.ofEpochMilli(oldestMessage.get()), Instant.ofEpochMilli(newestMessage.get()),
        targetDeleteBytes, cachedMessageBytes,
        outcomes.getLeft(), outcomes.getRight());
  }
}
