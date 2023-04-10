/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.SlotHash;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import org.whispersystems.textsecuregcm.util.Util;

public class ApnPushNotificationScheduler implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(ApnPushNotificationScheduler.class);

  private static final String PENDING_RECURRING_VOIP_NOTIFICATIONS_KEY_PREFIX = "PENDING_APN";
  private static final String PENDING_BACKGROUND_NOTIFICATIONS_KEY_PREFIX = "PENDING_BACKGROUND_APN";
  private static final String LAST_BACKGROUND_NOTIFICATION_TIMESTAMP_KEY_PREFIX = "LAST_BACKGROUND_NOTIFICATION";

  @VisibleForTesting
  static final String NEXT_SLOT_TO_PROCESS_KEY = "pending_notification_next_slot";

  private static final Counter delivered = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_delivered"));
  private static final Counter sent = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_sent"));
  private static final Counter retry = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_retry"));
  private static final Counter evicted = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_evicted"));

  private static final Counter backgroundNotificationScheduledCounter = Metrics.counter(name(ApnPushNotificationScheduler.class, "backgroundNotification", "scheduled"));
  private static final Counter backgroundNotificationSentCounter = Metrics.counter(name(ApnPushNotificationScheduler.class, "backgroundNotification", "sent"));

  private final APNSender apnSender;
  private final AccountsManager accountsManager;
  private final FaultTolerantRedisCluster pushSchedulingCluster;
  private final Clock clock;

  private final ClusterLuaScript getPendingVoipDestinationsScript;
  private final ClusterLuaScript insertPendingVoipDestinationScript;
  private final ClusterLuaScript removePendingVoipDestinationScript;

  private final ClusterLuaScript scheduleBackgroundNotificationScript;

  private final Thread[] workerThreads = new Thread[WORKER_THREAD_COUNT];

  private static final int WORKER_THREAD_COUNT = 4;

  @VisibleForTesting
  static final Duration BACKGROUND_NOTIFICATION_PERIOD = Duration.ofMinutes(20);

  private final AtomicBoolean running = new AtomicBoolean(false);

  class NotificationWorker implements Runnable {

    private static final int PAGE_SIZE = 128;

    @Override
    public void run() {
      while (running.get()) {
        try {
          final long entriesProcessed = processNextSlot();

          if (entriesProcessed == 0) {
            Util.sleep(1000);
          }
        } catch (Exception e) {
          logger.warn("Exception while operating", e);
        }
      }
    }

    private long processNextSlot() {
      final int slot = (int) (pushSchedulingCluster.withCluster(connection ->
          connection.sync().incr(NEXT_SLOT_TO_PROCESS_KEY)) % SlotHash.SLOT_COUNT);

      return processRecurringVoipNotifications(slot) + processScheduledBackgroundNotifications(slot);
    }

    @VisibleForTesting
    long processRecurringVoipNotifications(final int slot) {
      List<String> pendingDestinations;
      long entriesProcessed = 0;

      do {
        pendingDestinations = getPendingDestinationsForRecurringVoipNotifications(slot, PAGE_SIZE);
        entriesProcessed += pendingDestinations.size();

        for (final String destination : pendingDestinations) {
          try {
            getAccountAndDeviceFromPairString(destination).ifPresentOrElse(
                accountAndDevice -> sendRecurringVoipNotification(accountAndDevice.first(), accountAndDevice.second()),
                () -> removeRecurringVoipNotificationEntrySync(destination));
          } catch (final IllegalArgumentException e) {
            logger.warn("Failed to parse account/device pair: {}", destination, e);
          }
        }
      } while (!pendingDestinations.isEmpty());

      return entriesProcessed;
    }

    @VisibleForTesting
    long processScheduledBackgroundNotifications(final int slot) {
      final long currentTimeMillis = clock.millis();
      final String queueKey = getPendingBackgroundNotificationQueueKey(slot);

      final long processedBackgroundNotifications = pushSchedulingCluster.withCluster(connection -> {
        List<String> destinations;
        long offset = 0;

        do {
          destinations = connection.sync().zrangebyscore(queueKey, Range.create(0, currentTimeMillis), Limit.create(offset, PAGE_SIZE));

          for (final String destination : destinations) {
            try {
              getAccountAndDeviceFromPairString(destination).ifPresent(accountAndDevice ->
                  sendBackgroundNotification(accountAndDevice.first(), accountAndDevice.second()));
            } catch (final IllegalArgumentException e) {
              logger.warn("Failed to parse account/device pair: {}", destination, e);
            }
          }

          offset += destinations.size();
        } while (destinations.size() == PAGE_SIZE);

        return offset;
      });

      pushSchedulingCluster.useCluster(connection ->
          connection.sync().zremrangebyscore(queueKey, Range.create(0, currentTimeMillis)));

      return processedBackgroundNotifications;
    }
  }

  public ApnPushNotificationScheduler(FaultTolerantRedisCluster pushSchedulingCluster,
      APNSender apnSender,
      AccountsManager accountsManager) throws IOException {

    this(pushSchedulingCluster, apnSender, accountsManager, Clock.systemUTC());
  }

  @VisibleForTesting
  ApnPushNotificationScheduler(FaultTolerantRedisCluster pushSchedulingCluster,
      APNSender apnSender,
      AccountsManager accountsManager,
      Clock clock) throws IOException {

    this.apnSender = apnSender;
    this.accountsManager = accountsManager;
    this.pushSchedulingCluster = pushSchedulingCluster;
    this.clock = clock;

    this.getPendingVoipDestinationsScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/get.lua", ScriptOutputType.MULTI);
    this.insertPendingVoipDestinationScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/insert.lua", ScriptOutputType.VALUE);
    this.removePendingVoipDestinationScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/remove.lua", ScriptOutputType.INTEGER);

    this.scheduleBackgroundNotificationScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/schedule_background_notification.lua", ScriptOutputType.VALUE);

    for (int i = 0; i < this.workerThreads.length; i++) {
      this.workerThreads[i] = new Thread(new NotificationWorker(), "ApnFallbackManagerWorker-" + i);
    }
  }

  /**
   * Schedule a recurring VOIP notification until {@link this#cancelScheduledNotifications} is called or the device is
   * removed
   *
   * @return A CompletionStage that completes when the recurring notification has successfully been scheduled
   */
  public CompletionStage<Void> scheduleRecurringVoipNotification(Account account, Device device) {
    sent.increment();
    return insertRecurringVoipNotificationEntry(account, device, clock.millis() + (15 * 1000), (15 * 1000));
  }

  /**
   * Schedule a background notification to be sent some time in the future
   *
   * @return A CompletionStage that completes when the notification has successfully been scheduled
   */
  public CompletionStage<Void> scheduleBackgroundNotification(final Account account, final Device device) {
    backgroundNotificationScheduledCounter.increment();

    return scheduleBackgroundNotificationScript.executeAsync(
        List.of(
            getLastBackgroundNotificationTimestampKey(account, device),
            getPendingBackgroundNotificationQueueKey(account, device)),
        List.of(
            getPairString(account, device),
            String.valueOf(clock.millis()),
            String.valueOf(BACKGROUND_NOTIFICATION_PERIOD.toMillis())))
        .thenAccept(dropValue());
  }

  /**
   * Cancel a scheduled recurring VOIP notification
   *
   * @return A CompletionStage that completes when the scheduled task has been cancelled.
   */
  public CompletionStage<Void> cancelScheduledNotifications(Account account, Device device) {
    return removeRecurringVoipNotificationEntry(account, device)
        .thenCompose(removed -> {
          if (removed) {
            delivered.increment();
          }
          return pushSchedulingCluster.withCluster(connection ->
              connection.async().zrem(
                  getPendingBackgroundNotificationQueueKey(account, device),
                  getPairString(account, device)));
        })
        .thenAccept(dropValue());
  }

  @Override
  public synchronized void start() {
    running.set(true);

    for (final Thread workerThread : workerThreads) {
      workerThread.start();
    }
  }

  @Override
  public synchronized void stop() throws InterruptedException {
    running.set(false);

    for (final Thread workerThread : workerThreads) {
      workerThread.join();
    }
  }

  private void sendRecurringVoipNotification(final Account account, final Device device) {
    String apnId = device.getVoipApnId();

    if (apnId == null) {
      removeRecurringVoipNotificationEntrySync(getEndpointKey(account, device));
      return;
    }

    long deviceLastSeen = device.getLastSeen();
    if (deviceLastSeen < clock.millis() - TimeUnit.DAYS.toMillis(7)) {
      evicted.increment();
      removeRecurringVoipNotificationEntrySync(getEndpointKey(account, device));
      return;
    }

    apnSender.sendNotification(new PushNotification(apnId, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device, true));
    retry.increment();
  }

  @VisibleForTesting
  void sendBackgroundNotification(final Account account, final Device device) {
    if (StringUtils.isNotBlank(device.getApnId())) {
      // It's okay for the "last notification" timestamp to expire after the "cooldown" period has elapsed; a missing
      // timestamp and a timestamp older than the period are functionally equivalent.
      pushSchedulingCluster.useCluster(connection -> connection.sync().set(
          getLastBackgroundNotificationTimestampKey(account, device),
          String.valueOf(clock.millis()), new SetArgs().ex(BACKGROUND_NOTIFICATION_PERIOD)));

      apnSender.sendNotification(new PushNotification(device.getApnId(), PushNotification.TokenType.APN, PushNotification.NotificationType.NOTIFICATION, null, account, device, false));

      backgroundNotificationSentCounter.increment();
    }
  }

  @VisibleForTesting
  static Optional<Pair<String, Long>> getSeparated(String encoded) {
    try {
      if (encoded == null) return Optional.empty();

      String[] parts = encoded.split(":");

      if (parts.length != 2) {
        logger.warn("Got strange encoded number: " + encoded);
        return Optional.empty();
      }

      return Optional.of(new Pair<>(parts[0], Long.parseLong(parts[1])));
    } catch (NumberFormatException e) {
      logger.warn("Badly formatted: " + encoded, e);
      return Optional.empty();
    }
  }

  @VisibleForTesting
  static String getPairString(final Account account, final Device device) {
    return account.getUuid() + ":" + device.getId();
  }

  @VisibleForTesting
  Optional<Pair<Account, Device>> getAccountAndDeviceFromPairString(final String endpoint) {
    try {
      if (StringUtils.isBlank(endpoint)) {
        throw new IllegalArgumentException("Endpoint must not be blank");
      }

      final String[] parts = endpoint.split(":");

      if (parts.length != 2) {
        throw new IllegalArgumentException("Could not parse endpoint string: " + endpoint);
      }

      final Optional<Account> maybeAccount = accountsManager.getByAccountIdentifier(UUID.fromString(parts[0]));

      return maybeAccount.flatMap(account -> account.getDevice(Long.parseLong(parts[1])))
          .map(device -> new Pair<>(maybeAccount.get(), device));

    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private boolean removeRecurringVoipNotificationEntrySync(final String endpoint) {
    try {
      return removeRecurringVoipNotificationEntry(endpoint).toCompletableFuture().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RedisException re) {
        throw re;
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private CompletionStage<Boolean> removeRecurringVoipNotificationEntry(Account account, Device device) {
    return removeRecurringVoipNotificationEntry(getEndpointKey(account, device));
  }

  private CompletionStage<Boolean> removeRecurringVoipNotificationEntry(final String endpoint) {
    return removePendingVoipDestinationScript.executeAsync(
        List.of(getPendingRecurringVoipNotificationQueueKey(endpoint), endpoint),
        Collections.emptyList())
        .thenApply(result -> ((long) result) > 0);
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  List<String> getPendingDestinationsForRecurringVoipNotifications(final int slot, final int limit) {
    return (List<String>) getPendingVoipDestinationsScript.execute(
        List.of(getPendingRecurringVoipNotificationQueueKey(slot)),
        List.of(String.valueOf(clock.millis()), String.valueOf(limit)));
  }

  private CompletionStage<Void> insertRecurringVoipNotificationEntry(final Account account, final Device device, final long timestamp, final long interval) {
    final String endpoint = getEndpointKey(account, device);

    return insertPendingVoipDestinationScript.executeAsync(
        List.of(getPendingRecurringVoipNotificationQueueKey(endpoint), endpoint),
        List.of(String.valueOf(timestamp),
            String.valueOf(interval),
            account.getUuid().toString(),
            String.valueOf(device.getId())))
        .thenAccept(dropValue());
  }

  @VisibleForTesting
  static String getEndpointKey(final Account account, final Device device) {
    return "apn_device::{" + account.getUuid() + "::" + device.getId() + "}";
  }

  private static String getPendingRecurringVoipNotificationQueueKey(final String endpoint) {
    return getPendingRecurringVoipNotificationQueueKey(SlotHash.getSlot(endpoint));
  }

  private static String getPendingRecurringVoipNotificationQueueKey(final int slot) {
    return PENDING_RECURRING_VOIP_NOTIFICATIONS_KEY_PREFIX + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  @VisibleForTesting
  static String getPendingBackgroundNotificationQueueKey(final Account account, final Device device) {
    return getPendingBackgroundNotificationQueueKey(SlotHash.getSlot(getPairString(account, device)));
  }

  private static String getPendingBackgroundNotificationQueueKey(final int slot) {
    return PENDING_BACKGROUND_NOTIFICATIONS_KEY_PREFIX + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  private static String getLastBackgroundNotificationTimestampKey(final Account account, final Device device) {
    return LAST_BACKGROUND_NOTIFICATION_TIMESTAMP_KEY_PREFIX + "::{" + getPairString(account, device) + "}";
  }

  @VisibleForTesting
  Optional<Instant> getLastBackgroundNotificationTimestamp(final Account account, final Device device) {
    return Optional.ofNullable(
        pushSchedulingCluster.withCluster(connection ->
            connection.sync().get(getLastBackgroundNotificationTimestampKey(account, device))))
        .map(timestampString -> Instant.ofEpochMilli(Long.parseLong(timestampString)));
  }

  @VisibleForTesting
  Optional<Instant> getNextScheduledBackgroundNotificationTimestamp(final Account account, final Device device) {
    return Optional.ofNullable(
            pushSchedulingCluster.withCluster(connection ->
                connection.sync().zscore(getPendingBackgroundNotificationQueueKey(account, device),
                    getPairString(account, device))))
        .map(timestamp -> Instant.ofEpochMilli(timestamp.longValue()));
  }

  private static <T> Consumer<T> dropValue() {
    return ignored -> {};
  }
}
