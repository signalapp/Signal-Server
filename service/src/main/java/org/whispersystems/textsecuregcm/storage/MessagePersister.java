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
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.DevicePlatformUtil;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
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
  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final DisconnectionRequestManager disconnectionRequestManager;

  private final Duration persistDelay;

  private final Thread[] workerThreads;
  private volatile boolean running;

  private static final String OVERSIZED_QUEUE_COUNTER_NAME = name(MessagePersister.class, "persistQueueOversized");
  private static final String PERSISTED_MESSAGE_COUNTER_NAME = name(MessagePersister.class, "persistMessage");
  private static final String PERSISTED_BYTES_COUNTER_NAME = name(MessagePersister.class, "persistBytes");

  private static final Timer GET_QUEUES_TIMER = Metrics.timer(name(MessagePersister.class, "getQueues"));
  private static final Timer PERSIST_QUEUE_TIMER = Metrics.timer(name(MessagePersister.class, "persistQueue"));
  private static final Counter PERSIST_QUEUE_EXCEPTION_METER =
      Metrics.counter(name(MessagePersister.class, "persistQueueException"));
  private static final Counter TRIMMED_MESSAGE_COUNTER = Metrics.counter(name(MessagePersister.class, "trimmedMessage"));
  private static final Counter TRIMMED_MESSAGE_BYTES_COUNTER = Metrics.counter(name(MessagePersister.class, "trimmedMessageBytes"));

  private static final DistributionSummary QUEUE_COUNT_DISTRIBUTION_SUMMARY = DistributionSummary.builder(
          name(MessagePersister.class, "queueCount"))
      .publishPercentileHistogram(true)
      .register(Metrics.globalRegistry);

  private static final String QUEUE_SIZE_DISTRIBUTION_SUMMARY_NAME = name(MessagePersister.class, "queueSize");

  static final int QUEUE_BATCH_LIMIT = 100;
  static final int MESSAGE_BATCH_LIMIT = 100;

  private static final long EXCEPTION_PAUSE_MILLIS = Duration.ofSeconds(3).toMillis();

  private static final int CONSECUTIVE_EMPTY_CACHE_REMOVAL_LIMIT = 3;

  private static final Logger logger = LoggerFactory.getLogger(MessagePersister.class);

  public MessagePersister(final MessagesCache messagesCache,
      final MessagesManager messagesManager,
      final AccountsManager accountsManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final DisconnectionRequestManager disconnectionRequestManager,
      final Duration persistDelay,
      final int dedicatedProcessWorkerThreadCount) {

    this.messagesCache = messagesCache;
    this.messagesManager = messagesManager;
    this.accountsManager = accountsManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.disconnectionRequestManager = disconnectionRequestManager;
    this.persistDelay = persistDelay;
    this.workerThreads = new Thread[dedicatedProcessWorkerThreadCount];

    for (int i = 0; i < workerThreads.length; i++) {
      workerThreads[i] = new Thread(() -> {
        while (running) {
          if (dynamicConfigurationManager.getConfiguration().getMessagePersisterConfiguration()
              .isPersistenceEnabled()) {
            try {
              final int queuesPersisted = persistNextQueues(Instant.now());
              QUEUE_COUNT_DISTRIBUTION_SUMMARY.record(queuesPersisted);

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
    final String shard = messagesCache.shardForSlot(slot);

    List<String> queuesToPersist;
    int queuesPersisted = 0;

    do {
      queuesToPersist = GET_QUEUES_TIMER.record(
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
          persistQueue(maybeAccount.get(), maybeDevice.get(), shard);
        } catch (final Exception e) {
          PERSIST_QUEUE_EXCEPTION_METER.increment();
          logger.warn("Failed to persist queue {}::{} (slot {}, shard {}); will schedule for retry",
              accountUuid, deviceId, slot, shard, e);

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
  void persistQueue(final Account account, final Device device, final String shard) throws MessagePersistenceException {
    final UUID accountUuid = account.getUuid();
    final byte deviceId = device.getId();

    final Tag platformTag = Tag.of("platform", DevicePlatformUtil.getDevicePlatform(device)
        .map(platform -> platform.name().toLowerCase(Locale.ROOT))
        .orElse("unknown"));

    final Timer.Sample sample = Timer.start();

    messagesCache.lockQueueForPersistence(accountUuid, deviceId);

    try {
      int messageCount = 0;
      List<MessageProtos.Envelope> messages;

      int consecutiveEmptyCacheRemovals = 0;

      do {
        messages = messagesCache.getMessagesToPersist(accountUuid, deviceId, MESSAGE_BATCH_LIMIT);

        final int urgentMessageCount = (int) messages.stream().filter(MessageProtos.Envelope::getUrgent).count();
        final int nonUrgentMessageCount = messages.size() - urgentMessageCount;

        final Tags tags = Tags.of(platformTag, Tag.of("shard", shard));

        Metrics.counter(PERSISTED_MESSAGE_COUNTER_NAME, tags.and("urgent", "true")).increment(urgentMessageCount);
        Metrics.counter(PERSISTED_MESSAGE_COUNTER_NAME, tags.and("urgent", "false")).increment(nonUrgentMessageCount);
        Metrics.counter(PERSISTED_BYTES_COUNTER_NAME, tags)
            .increment(messages.stream().mapToInt(MessageProtos.Envelope::getSerializedSize).sum());

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

      DistributionSummary.builder(QUEUE_SIZE_DISTRIBUTION_SUMMARY_NAME)
          .tags(Tags.of(platformTag))
          .publishPercentileHistogram(true)
          .register(Metrics.globalRegistry)
          .record(messageCount);
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
        logger.warn("Failed to persist queue {}::{} due to overfull queue; will unlink device", accountUuid, deviceId);
        accountsManager.removeDevice(account, deviceId)
            .thenRun(() -> disconnectionRequestManager.requestDisconnection(accountUuid))
            .join();
      }
    } finally {
      messagesCache.unlockQueueForPersistence(accountUuid, deviceId);
      sample.stop(PERSIST_QUEUE_TIMER);
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
    final long cachedMessageBytes = messagesCache.estimatePersistedQueueSizeBytes(aci, deviceId).join();
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
          TRIMMED_MESSAGE_COUNTER.increment();
          TRIMMED_MESSAGE_BYTES_COUNTER.increment(envelope.getSerializedSize());
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
