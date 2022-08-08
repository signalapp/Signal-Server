/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.SlotHash;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class ApnPushNotificationScheduler implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(ApnPushNotificationScheduler.class);

  private static final String PENDING_NOTIFICATIONS_KEY = "PENDING_APN";

  @VisibleForTesting
  static final String NEXT_SLOT_TO_PERSIST_KEY  = "pending_notification_next_slot";

  private static final Counter delivered = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_delivered"));
  private static final Counter sent = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_sent"));
  private static final Counter retry = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_retry"));
  private static final Counter evicted = Metrics.counter(name(ApnPushNotificationScheduler.class, "voip_evicted"));

  private final APNSender apnSender;
  private final AccountsManager accountsManager;
  private final FaultTolerantRedisCluster pushSchedulingCluster;
  private final Clock clock;

  private final ClusterLuaScript getPendingVoipDestinationsScript;
  private final ClusterLuaScript insertPendingVoipDestinationScript;
  private final ClusterLuaScript removePendingVoipDestinationScript;

  private final Thread[] workerThreads = new Thread[WORKER_THREAD_COUNT];

  private static final int WORKER_THREAD_COUNT = 4;

  private final AtomicBoolean running = new AtomicBoolean(false);

  class NotificationWorker implements Runnable {

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

    long processNextSlot() {
      final int slot = getNextSlot();

      List<String> pendingDestinations;
      long entriesProcessed = 0;

      do {
        pendingDestinations = getPendingDestinationsForRecurringVoipNotifications(slot, 100);
        entriesProcessed += pendingDestinations.size();

        for (final String uuidAndDevice : pendingDestinations) {
          final Optional<Pair<String, Long>> separated = getSeparated(uuidAndDevice);

          final Optional<Account> maybeAccount = separated.map(Pair::first)
                                                          .map(UUID::fromString)
                                                          .flatMap(accountsManager::getByAccountIdentifier);

          final Optional<Device> maybeDevice = separated.map(Pair::second)
                                                        .flatMap(deviceId -> maybeAccount.flatMap(account -> account.getDevice(deviceId)));

          if (maybeAccount.isPresent() && maybeDevice.isPresent()) {
            sendRecurringVoipNotification(maybeAccount.get(), maybeDevice.get());
          } else {
            removeRecurringVoipNotificationEntry(uuidAndDevice);
          }
        }
      } while (!pendingDestinations.isEmpty());

      return entriesProcessed;
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

    for (int i = 0; i < this.workerThreads.length; i++) {
      this.workerThreads[i] = new Thread(new NotificationWorker(), "ApnFallbackManagerWorker-" + i);
    }
  }

  public void scheduleRecurringVoipNotification(Account account, Device device) {
    sent.increment();
    insertRecurringVoipNotificationEntry(account, device, clock.millis() + (15 * 1000), (15 * 1000));
  }

  public void cancelRecurringVoipNotification(Account account, Device device) {
    if (removeRecurringVoipNotificationEntry(account, device)) {
      delivered.increment();
    }
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
      removeRecurringVoipNotificationEntry(account, device);
      return;
    }

    long deviceLastSeen = device.getLastSeen();

    if (deviceLastSeen < clock.millis() - TimeUnit.DAYS.toMillis(7)) {
      evicted.increment();
      removeRecurringVoipNotificationEntry(account, device);
      return;
    }

    apnSender.sendNotification(new PushNotification(apnId, PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device));
    retry.increment();
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

  private boolean removeRecurringVoipNotificationEntry(Account account, Device device) {
    return removeRecurringVoipNotificationEntry(getEndpointKey(account, device));
  }

  private boolean removeRecurringVoipNotificationEntry(final String endpoint) {
    return (long) removePendingVoipDestinationScript.execute(
        List.of(getPendingRecurringVoipNotificationQueueKey(endpoint), endpoint),
        Collections.emptyList()) > 0;
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  List<String> getPendingDestinationsForRecurringVoipNotifications(final int slot, final int limit) {
    return (List<String>) getPendingVoipDestinationsScript.execute(
        List.of(getPendingRecurringVoipNotificationQueueKey(slot)),
        List.of(String.valueOf(clock.millis()), String.valueOf(limit)));
  }

  private void insertRecurringVoipNotificationEntry(final Account account, final Device device, final long timestamp, final long interval) {
    final String endpoint = getEndpointKey(account, device);

    insertPendingVoipDestinationScript.execute(
        List.of(getPendingRecurringVoipNotificationQueueKey(endpoint), endpoint),
        List.of(String.valueOf(timestamp),
            String.valueOf(interval),
            account.getUuid().toString(),
            String.valueOf(device.getId())));
  }

  @VisibleForTesting
  String getEndpointKey(final Account account, final Device device) {
    return "apn_device::{" + account.getUuid() + "::" + device.getId() + "}";
  }

  private String getPendingRecurringVoipNotificationQueueKey(final String endpoint) {
    return getPendingRecurringVoipNotificationQueueKey(SlotHash.getSlot(endpoint));
  }

  private String getPendingRecurringVoipNotificationQueueKey(final int slot) {
    return PENDING_NOTIFICATIONS_KEY + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  private int getNextSlot() {
    return (int)(pushSchedulingCluster.withCluster(connection -> connection.sync().incr(NEXT_SLOT_TO_PERSIST_KEY)) % SlotHash.SLOT_COUNT);
  }
}
