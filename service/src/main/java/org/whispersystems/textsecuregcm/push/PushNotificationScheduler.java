/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.Range;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class PushNotificationScheduler implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(PushNotificationScheduler.class);

  private static final String PENDING_RECURRING_VOIP_NOTIFICATIONS_KEY_PREFIX = "PENDING_APN";
  private static final String PENDING_BACKGROUND_NOTIFICATIONS_KEY_PREFIX = "PENDING_BACKGROUND_APN";
  private static final String LAST_BACKGROUND_NOTIFICATION_TIMESTAMP_KEY_PREFIX = "LAST_BACKGROUND_NOTIFICATION";
  private static final String PENDING_DELAYED_NOTIFICATIONS_KEY_PREFIX = "DELAYED";

  @VisibleForTesting
  static final String NEXT_SLOT_TO_PROCESS_KEY = "pending_notification_next_slot";

  private static final Counter delivered = Metrics.counter("chat.ApnPushNotificationScheduler.voip_delivered");
  private static final Counter sent = Metrics.counter("chat.ApnPushNotificationScheduler.voip_sent");
  private static final Counter retry = Metrics.counter("chat.ApnPushNotificationScheduler.voip_retry");
  private static final Counter evicted = Metrics.counter("chat.ApnPushNotificationScheduler.voip_evicted");

  private static final Counter BACKGROUND_NOTIFICATION_SCHEDULED_COUNTER = Metrics.counter(name(PushNotificationScheduler.class, "backgroundNotification", "scheduled"));
  private static final String BACKGROUND_NOTIFICATION_SENT_COUNTER_NAME = name(PushNotificationScheduler.class, "backgroundNotification", "sent");

  private static final String DELAYED_NOTIFICATION_SCHEDULED_COUNTER_NAME = name(PushNotificationScheduler.class, "delayedNotificationScheduled");
  private static final String DELAYED_NOTIFICATION_SENT_COUNTER_NAME = name(PushNotificationScheduler.class, "delayedNotificationSent");
  private static final String TOKEN_TYPE_TAG = "tokenType";
  private static final String ACCEPTED_TAG = "accepted";

  private final APNSender apnSender;
  private final FcmSender fcmSender;
  private final AccountsManager accountsManager;
  private final FaultTolerantRedisCluster pushSchedulingCluster;
  private final Clock clock;

  private final ClusterLuaScript getPendingVoipDestinationsScript;
  private final ClusterLuaScript insertPendingVoipDestinationScript;
  private final ClusterLuaScript removePendingVoipDestinationScript;

  private final ClusterLuaScript scheduleBackgroundApnsNotificationScript;

  private final Thread[] workerThreads;

  @VisibleForTesting
  static final Duration BACKGROUND_NOTIFICATION_PERIOD = Duration.ofMinutes(20);

  private final AtomicBoolean running = new AtomicBoolean(false);

  class NotificationWorker implements Runnable {

    private final int maxConcurrency;

    private static final int PAGE_SIZE = 128;

    NotificationWorker(final int maxConcurrency) {
      this.maxConcurrency = maxConcurrency;
    }

    @Override
    public void run() {
      do {
        try {
          final long entriesProcessed = processNextSlot();

          if (entriesProcessed == 0) {
            Util.sleep(1000);
          }
        } catch (Exception e) {
          logger.warn("Exception while operating", e);
        }
      } while (running.get());
    }

    private long processNextSlot() {
      final int slot = (int) (pushSchedulingCluster.withCluster(connection ->
          connection.sync().incr(NEXT_SLOT_TO_PROCESS_KEY)) % SlotHash.SLOT_COUNT);

      return processRecurringApnsVoipNotifications(slot) +
          processScheduledBackgroundApnsNotifications(slot) +
          processScheduledDelayedNotifications(slot);
    }

    @VisibleForTesting
    long processRecurringApnsVoipNotifications(final int slot) {
      List<String> pendingDestinations;
      long entriesProcessed = 0;

      do {
        pendingDestinations = getPendingDestinationsForRecurringApnsVoipNotifications(slot, PAGE_SIZE);
        entriesProcessed += pendingDestinations.size();

        Flux.fromIterable(pendingDestinations)
            .flatMap(destination -> Mono.fromFuture(() -> getAccountAndDeviceFromPairString(destination))
                .flatMap(maybeAccountAndDevice -> {
                  if (maybeAccountAndDevice.isPresent()) {
                    final Pair<Account, Device> accountAndDevice = maybeAccountAndDevice.get();
                    return Mono.fromFuture(() -> sendRecurringApnsVoipNotification(accountAndDevice.first(), accountAndDevice.second()));
                  } else {
                    final Pair<UUID, Byte> aciAndDeviceId = decodeAciAndDeviceId(destination);
                    return Mono.fromFuture(() -> removeRecurringApnsVoipNotificationEntry(aciAndDeviceId.first(), aciAndDeviceId.second()))
                        .then();
                  }
                }), maxConcurrency)
            .then()
            .block();
      } while (!pendingDestinations.isEmpty());

      return entriesProcessed;
    }

    @VisibleForTesting
    long processScheduledBackgroundApnsNotifications(final int slot) {
      return processScheduledNotifications(getPendingBackgroundApnsNotificationQueueKey(slot),
          PushNotificationScheduler.this::sendBackgroundApnsNotification);
    }

    @VisibleForTesting
    long processScheduledDelayedNotifications(final int slot) {
      return processScheduledNotifications(getDelayedNotificationQueueKey(slot),
          PushNotificationScheduler.this::sendDelayedNotification);
    }

    private long processScheduledNotifications(final String queueKey,
        final BiFunction<Account, Device, CompletableFuture<Void>> sendNotificationFunction) {

      final long currentTimeMillis = clock.millis();
      final AtomicLong processedNotifications = new AtomicLong(0);

      pushSchedulingCluster.useCluster(
          connection -> connection.reactive().zrangebyscore(queueKey, Range.create(0, currentTimeMillis))
              .flatMap(encodedAciAndDeviceId -> Mono.fromFuture(
                  () -> getAccountAndDeviceFromPairString(encodedAciAndDeviceId)), maxConcurrency)
              .flatMap(Mono::justOrEmpty)
              .flatMap(accountAndDevice -> Mono.fromFuture(
                          () -> sendNotificationFunction.apply(accountAndDevice.first(), accountAndDevice.second()))
                      .then(Mono.defer(() -> connection.reactive().zrem(queueKey, encodeAciAndDeviceId(accountAndDevice.first(), accountAndDevice.second()))))
                      .doOnSuccess(ignored -> processedNotifications.incrementAndGet()),
                  maxConcurrency)
              .then()
              .block());

      return processedNotifications.get();
    }
  }

  public PushNotificationScheduler(final FaultTolerantRedisCluster pushSchedulingCluster,
      final APNSender apnSender,
      final FcmSender fcmSender,
      final AccountsManager accountsManager,
      final int dedicatedProcessWorkerThreadCount,
      final int workerMaxConcurrency) throws IOException {

    this(pushSchedulingCluster,
        apnSender,
        fcmSender,
        accountsManager,
        Clock.systemUTC(),
        dedicatedProcessWorkerThreadCount,
        workerMaxConcurrency);
  }

  @VisibleForTesting
  PushNotificationScheduler(final FaultTolerantRedisCluster pushSchedulingCluster,
                            final APNSender apnSender,
                            final FcmSender fcmSender,
                            final AccountsManager accountsManager,
                            final Clock clock,
                            final int dedicatedProcessThreadCount,
                            final int workerMaxConcurrency) throws IOException {

    this.apnSender = apnSender;
    this.fcmSender = fcmSender;
    this.accountsManager = accountsManager;
    this.pushSchedulingCluster = pushSchedulingCluster;
    this.clock = clock;

    this.getPendingVoipDestinationsScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/get.lua",
        ScriptOutputType.MULTI);
    this.insertPendingVoipDestinationScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/insert.lua",
        ScriptOutputType.VALUE);
    this.removePendingVoipDestinationScript = ClusterLuaScript.fromResource(pushSchedulingCluster, "lua/apn/remove.lua",
        ScriptOutputType.INTEGER);

    this.scheduleBackgroundApnsNotificationScript = ClusterLuaScript.fromResource(pushSchedulingCluster,
        "lua/apn/schedule_background_notification.lua", ScriptOutputType.VALUE);

    this.workerThreads = new Thread[dedicatedProcessThreadCount];

    for (int i = 0; i < this.workerThreads.length; i++) {
      this.workerThreads[i] = new Thread(new NotificationWorker(workerMaxConcurrency), "PushNotificationScheduler-" + i);
    }
  }

  /**
   * Schedule a recurring VOIP notification until {@link this#cancelScheduledNotifications} is called or the device is
   * removed
   *
   * @return A CompletionStage that completes when the recurring notification has successfully been scheduled
   */
  public CompletionStage<Void> scheduleRecurringApnsVoipNotification(Account account, Device device) {
    sent.increment();
    return insertRecurringApnsVoipNotificationEntry(account.getIdentifier(IdentityType.ACI), device.getId(), clock.millis() + (15 * 1000), (15 * 1000));
  }

  /**
   * Schedule a background APNs notification to be sent some time in the future.
   *
   * @return A CompletionStage that completes when the notification has successfully been scheduled
   *
   * @throws IllegalArgumentException if the given device does not have an APNs token
   */
  public CompletionStage<Void> scheduleBackgroundApnsNotification(final Account account, final Device device) {
    if (StringUtils.isBlank(device.getApnId())) {
      throw new IllegalArgumentException("Device must have an APNs token");
    }

    BACKGROUND_NOTIFICATION_SCHEDULED_COUNTER.increment();

    return scheduleBackgroundApnsNotificationScript.executeAsync(
        List.of(
            getLastBackgroundApnsNotificationTimestampKey(account, device),
            getPendingBackgroundApnsNotificationQueueKey(account, device)),
        List.of(
            encodeAciAndDeviceId(account, device),
            String.valueOf(clock.millis()),
            String.valueOf(BACKGROUND_NOTIFICATION_PERIOD.toMillis())))
        .thenRun(Util.NOOP);
  }

  /**
   * Schedules a "new message" push notification to be delivered to the given device after at least the given duration.
   * If another notification had previously been scheduled, calling this method will replace the previously-scheduled
   * delivery time with the given time.
   *
   * @param account the account to which the target device belongs
   * @param device the device to which to deliver a "new message" push notification
   * @param minDelay the minimum delay after which to deliver the notification
   *
   * @return a future that completes once the notification has been scheduled
   */
  public CompletableFuture<Void> scheduleDelayedNotification(final Account account, final Device device, final Duration minDelay) {
    return pushSchedulingCluster.withCluster(connection ->
        connection.async().zadd(getDelayedNotificationQueueKey(account, device),
            clock.instant().plus(minDelay).toEpochMilli(),
            encodeAciAndDeviceId(account, device)))
        .thenRun(() -> Metrics.counter(DELAYED_NOTIFICATION_SCHEDULED_COUNTER_NAME,
                TOKEN_TYPE_TAG, getTokenType(device))
            .increment())
        .toCompletableFuture();
  }

  /**
   * Cancel a scheduled recurring VOIP notification
   *
   * @return A CompletionStage that completes when the scheduled task has been cancelled.
   */
  public CompletionStage<Void> cancelScheduledNotifications(Account account, Device device) {
    return CompletableFuture.allOf(
        cancelRecurringApnsVoipNotifications(account, device),
        cancelBackgroundApnsNotifications(account, device),
        cancelDelayedNotifications(account, device));
  }

  private CompletableFuture<Void> cancelRecurringApnsVoipNotifications(final Account account, final Device device) {
    return removeRecurringApnsVoipNotificationEntry(account.getIdentifier(IdentityType.ACI), device.getId())
        .thenCompose(removed -> {
          if (removed) {
            delivered.increment();
          }
          return pushSchedulingCluster.withCluster(connection ->
              connection.async().zrem(
                  getPendingBackgroundApnsNotificationQueueKey(account, device),
                  encodeAciAndDeviceId(account, device)));
        })
        .thenRun(Util.NOOP)
        .toCompletableFuture();
  }

  @VisibleForTesting
  CompletableFuture<Void> cancelBackgroundApnsNotifications(final Account account, final Device device) {
    return pushSchedulingCluster.withCluster(connection -> connection.async()
            .zrem(getPendingBackgroundApnsNotificationQueueKey(account, device), encodeAciAndDeviceId(account, device)))
        .thenRun(Util.NOOP)
        .toCompletableFuture();
  }

  @VisibleForTesting
  CompletableFuture<Void> cancelDelayedNotifications(final Account account, final Device device) {
    return pushSchedulingCluster.withCluster(connection ->
            connection.async().zrem(getDelayedNotificationQueueKey(account, device),
                encodeAciAndDeviceId(account, device)))
        .thenRun(Util.NOOP)
        .toCompletableFuture();
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

  private CompletableFuture<Void> sendRecurringApnsVoipNotification(final Account account, final Device device) {
    if (StringUtils.isBlank(device.getVoipApnId())) {
      return removeRecurringApnsVoipNotificationEntry(account.getIdentifier(IdentityType.ACI), device.getId())
          .thenRun(Util.NOOP);
    }

    if (device.getLastSeen() < clock.millis() - TimeUnit.DAYS.toMillis(7)) {
      return removeRecurringApnsVoipNotificationEntry(account.getIdentifier(IdentityType.ACI), device.getId())
          .thenRun(evicted::increment);
    }

    return apnSender.sendNotification(new PushNotification(device.getVoipApnId(), PushNotification.TokenType.APN_VOIP, PushNotification.NotificationType.NOTIFICATION, null, account, device, true))
        .thenRun(retry::increment);
  }

  @VisibleForTesting
  CompletableFuture<Void> sendBackgroundApnsNotification(final Account account, final Device device) {
    if (StringUtils.isBlank(device.getApnId())) {
      return CompletableFuture.completedFuture(null);
    }

    // It's okay for the "last notification" timestamp to expire after the "cooldown" period has elapsed; a missing
    // timestamp and a timestamp older than the period are functionally equivalent.
    return pushSchedulingCluster.withCluster(connection -> connection.async().set(
        getLastBackgroundApnsNotificationTimestampKey(account, device),
        String.valueOf(clock.millis()), new SetArgs().ex(BACKGROUND_NOTIFICATION_PERIOD)))
        .thenCompose(ignored -> apnSender.sendNotification(new PushNotification(device.getApnId(), PushNotification.TokenType.APN, PushNotification.NotificationType.NOTIFICATION, null, account, device, false)))
        .thenAccept(response -> Metrics.counter(BACKGROUND_NOTIFICATION_SENT_COUNTER_NAME,
                ACCEPTED_TAG, String.valueOf(response.accepted()))
            .increment())
        .toCompletableFuture();
  }

  @VisibleForTesting
  CompletableFuture<Void> sendDelayedNotification(final Account account, final Device device) {
    if (StringUtils.isAllBlank(device.getApnId(), device.getGcmId())) {
      return CompletableFuture.completedFuture(null);
    }

    final boolean isApnsDevice = StringUtils.isNotBlank(device.getApnId());

    final PushNotification pushNotification = new PushNotification(
        isApnsDevice ? device.getApnId() : device.getGcmId(),
        isApnsDevice ? PushNotification.TokenType.APN : PushNotification.TokenType.FCM,
        PushNotification.NotificationType.NOTIFICATION,
        null,
        account,
        device,
        true);

    final PushNotificationSender pushNotificationSender = isApnsDevice ? apnSender : fcmSender;

    return pushNotificationSender.sendNotification(pushNotification)
        .thenAccept(response -> Metrics.counter(DELAYED_NOTIFICATION_SENT_COUNTER_NAME,
            TOKEN_TYPE_TAG, getTokenType(device),
            ACCEPTED_TAG, String.valueOf(response.accepted()))
            .increment());
  }

  @VisibleForTesting
  static String encodeAciAndDeviceId(final Account account, final Device device) {
    return account.getUuid() + ":" + device.getId();
  }

  static Pair<UUID, Byte> decodeAciAndDeviceId(final String encoded) {
    if (StringUtils.isBlank(encoded)) {
      throw new IllegalArgumentException("Encoded ACI/device ID pair must not be blank");
    }

    final int separatorIndex = encoded.indexOf(':');

    if (separatorIndex == -1) {
      throw new IllegalArgumentException("String did not contain a ':' separator");
    }

    final UUID aci = UUID.fromString(encoded.substring(0, separatorIndex));
    final byte deviceId = Byte.parseByte(encoded.substring(separatorIndex + 1));

    return new Pair<>(aci, deviceId);
  }

  @VisibleForTesting
  CompletableFuture<Optional<Pair<Account, Device>>> getAccountAndDeviceFromPairString(final String endpoint) {
    final Pair<UUID, Byte> aciAndDeviceId = decodeAciAndDeviceId(endpoint);

    return accountsManager.getByAccountIdentifierAsync(aciAndDeviceId.first())
        .thenApply(maybeAccount -> maybeAccount
            .flatMap(account -> account.getDevice(aciAndDeviceId.second()).map(device -> new Pair<>(account, device))));
  }

  private CompletableFuture<Boolean> removeRecurringApnsVoipNotificationEntry(final UUID aci, final byte deviceId) {
    final String endpoint = getVoipEndpointKey(aci, deviceId);

    return removePendingVoipDestinationScript.executeAsync(
        List.of(getPendingRecurringApnsVoipNotificationQueueKey(endpoint), endpoint), Collections.emptyList())
        .thenApply(result -> ((long) result) > 0);
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  List<String> getPendingDestinationsForRecurringApnsVoipNotifications(final int slot, final int limit) {
    return (List<String>) getPendingVoipDestinationsScript.execute(
        List.of(getPendingRecurringApnsVoipNotificationQueueKey(slot)),
        List.of(String.valueOf(clock.millis()), String.valueOf(limit)));
  }

  @SuppressWarnings("SameParameterValue")
  private CompletionStage<Void> insertRecurringApnsVoipNotificationEntry(final UUID aci, final byte deviceId, final long timestamp, final long interval) {
    final String endpoint = getVoipEndpointKey(aci, deviceId);

    return insertPendingVoipDestinationScript.executeAsync(
        List.of(getPendingRecurringApnsVoipNotificationQueueKey(endpoint), endpoint),
        List.of(String.valueOf(timestamp),
            String.valueOf(interval),
            aci.toString(),
            String.valueOf(deviceId)))
        .thenRun(Util.NOOP);
  }

  @VisibleForTesting
  static String getVoipEndpointKey(final UUID aci, final byte deviceId) {
    return "apn_device::{" + aci + "::" + deviceId + "}";
  }

  private static String getPendingRecurringApnsVoipNotificationQueueKey(final String endpoint) {
    return getPendingRecurringApnsVoipNotificationQueueKey(SlotHash.getSlot(endpoint));
  }

  private static String getPendingRecurringApnsVoipNotificationQueueKey(final int slot) {
    return PENDING_RECURRING_VOIP_NOTIFICATIONS_KEY_PREFIX + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  @VisibleForTesting
  static String getPendingBackgroundApnsNotificationQueueKey(final Account account, final Device device) {
    return getPendingBackgroundApnsNotificationQueueKey(SlotHash.getSlot(encodeAciAndDeviceId(account, device)));
  }

  private static String getPendingBackgroundApnsNotificationQueueKey(final int slot) {
    return PENDING_BACKGROUND_NOTIFICATIONS_KEY_PREFIX + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  private static String getLastBackgroundApnsNotificationTimestampKey(final Account account, final Device device) {
    return LAST_BACKGROUND_NOTIFICATION_TIMESTAMP_KEY_PREFIX + "::{" + encodeAciAndDeviceId(account, device) + "}";
  }

  @VisibleForTesting
  static String getDelayedNotificationQueueKey(final Account account, final Device device) {
    return getDelayedNotificationQueueKey(SlotHash.getSlot(encodeAciAndDeviceId(account, device)));
  }

  private static String getDelayedNotificationQueueKey(final int slot) {
    return PENDING_DELAYED_NOTIFICATIONS_KEY_PREFIX + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  @VisibleForTesting
  Optional<Instant> getLastBackgroundApnsNotificationTimestamp(final Account account, final Device device) {
    return Optional.ofNullable(
        pushSchedulingCluster.withCluster(connection ->
            connection.sync().get(getLastBackgroundApnsNotificationTimestampKey(account, device))))
        .map(timestampString -> Instant.ofEpochMilli(Long.parseLong(timestampString)));
  }

  @VisibleForTesting
  Optional<Instant> getNextScheduledBackgroundApnsNotificationTimestamp(final Account account, final Device device) {
    return Optional.ofNullable(
            pushSchedulingCluster.withCluster(connection ->
                connection.sync().zscore(getPendingBackgroundApnsNotificationQueueKey(account, device),
                    encodeAciAndDeviceId(account, device))))
        .map(timestamp -> Instant.ofEpochMilli(timestamp.longValue()));
  }

  @VisibleForTesting
  Optional<Instant> getNextScheduledDelayedNotificationTimestamp(final Account account, final Device device) {
    return Optional.ofNullable(
            pushSchedulingCluster.withCluster(connection ->
                connection.sync().zscore(getDelayedNotificationQueueKey(account, device),
                    encodeAciAndDeviceId(account, device))))
        .map(timestamp -> Instant.ofEpochMilli(timestamp.longValue()));
  }

  private static String getTokenType(final Device device) {
    if (StringUtils.isNotBlank(device.getApnId())) {
      return "apns";
    } else if (StringUtils.isNotBlank(device.getGcmId())) {
      return "fcm";
    } else {
      return "unknown";
    }
  }
}
