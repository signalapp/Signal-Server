/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.SlotHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.RedisException;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import org.whispersystems.textsecuregcm.util.Util;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class ApnFallbackManager implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(ApnFallbackManager.class);

  private static final String SINGLETON_PENDING_NOTIFICATIONS_KEY = "PENDING_APN";
          static final String NEXT_SLOT_TO_PERSIST_KEY            = "pending_notification_next_slot";

  private static final MetricRegistry metricRegistry        = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter          delivered             = metricRegistry.meter(name(ApnFallbackManager.class, "voip_delivered"));
  private static final Meter          sent                  = metricRegistry.meter(name(ApnFallbackManager.class, "voip_sent"     ));
  private static final Meter          retry                 = metricRegistry.meter(name(ApnFallbackManager.class, "voip_retry"));
  private static final Meter          evicted               = metricRegistry.meter(name(ApnFallbackManager.class, "voip_evicted"));
  private static final Meter          singletonDestinations = metricRegistry.meter(name(ApnFallbackManager.class, "singleton_destinations"));
  private static final Meter          clusterDestinations   = metricRegistry.meter(name(ApnFallbackManager.class, "cluster_destinations"));

  static {
    metricRegistry.register(name(ApnFallbackManager.class, "voip_ratio"), new VoipRatioGauge(delivered, sent));
  }

  private final APNSender                 apnSender;
  private final AccountsManager           accountsManager;
  private final FaultTolerantRedisCluster cluster;

  private final LuaScript getSingletonScript;
  private final LuaScript removeSingletonScript;

  private final ClusterLuaScript getClusterScript;
  private final ClusterLuaScript insertClusterScript;
  private final ClusterLuaScript removeClusterScript;

  private final Thread   singletonWorkerThread;
  private final Thread[] clusterWorkerThreads = new Thread[CLUSTER_WORKER_THREAD_COUNT];

  private static final int CLUSTER_WORKER_THREAD_COUNT = 4;

  private final AtomicBoolean running = new AtomicBoolean(false);

  private class SingletonCacheWorker implements Runnable {

    @Override
    public void run() {
      while (running.get()) {
        try {
          for (final String numberAndDevice : getPendingDestinationsFromSingletonCache(100)) {
            singletonDestinations.mark();

            Optional<Pair<String, Long>> separated = getSeparated(numberAndDevice);

            if (!separated.isPresent()) {
              removeFromSingleton(numberAndDevice);
              continue;
            }

            Optional<Account> account = accountsManager.get(separated.get().first());

            if (!account.isPresent()) {
              removeFromSingleton(numberAndDevice);
              continue;
            }

            Optional<Device> device = account.get().getDevice(separated.get().second());

            if (!device.isPresent()) {
              removeFromSingleton(numberAndDevice);
              continue;
            }

            sendNotification(account.get(), device.get());
          }

        } catch (Exception e) {
          logger.warn("Exception while operating", e);
        }

        Util.sleep(1000);
      }
    }
  }

  class ClusterCacheWorker implements Runnable {

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
        pendingDestinations = getPendingDestinationsFromClusterCache(slot, 100);
        entriesProcessed += pendingDestinations.size();

        for (final String uuidAndDevice : pendingDestinations) {
          clusterDestinations.mark();

          final Optional<Pair<String, Long>> separated = getSeparated(uuidAndDevice);

          final Optional<Account> maybeAccount = separated.map(Pair::first)
                                                          .map(UUID::fromString)
                                                          .flatMap(accountsManager::get);

          final Optional<Device> maybeDevice = separated.map(Pair::second)
                                                        .flatMap(deviceId -> maybeAccount.flatMap(account -> account.getDevice(deviceId)));

          if (maybeAccount.isPresent() && maybeDevice.isPresent()) {
            sendNotification(maybeAccount.get(), maybeDevice.get());
          } else {
            removeFromCluster(uuidAndDevice);
          }
        }
      } while (!pendingDestinations.isEmpty());

      return entriesProcessed;
    }
  }

  public ApnFallbackManager(ReplicatedJedisPool jedisPool,
                            FaultTolerantRedisCluster cluster,
                            APNSender apnSender,
                            AccountsManager accountsManager)
      throws IOException
  {
    this.apnSender       = apnSender;
    this.accountsManager = accountsManager;
    this.cluster         = cluster;

    this.getSingletonScript    = LuaScript.fromResource(jedisPool, "lua/apn/get.lua");
    this.removeSingletonScript = LuaScript.fromResource(jedisPool, "lua/apn/remove.lua");

    this.getClusterScript    = ClusterLuaScript.fromResource(cluster, "lua/apn/get.lua", ScriptOutputType.MULTI);
    this.insertClusterScript = ClusterLuaScript.fromResource(cluster, "lua/apn/insert.lua", ScriptOutputType.VALUE);
    this.removeClusterScript = ClusterLuaScript.fromResource(cluster, "lua/apn/remove.lua", ScriptOutputType.INTEGER);

    this.singletonWorkerThread = new Thread(new SingletonCacheWorker(), "ApnFallbackManagerSingletonWorker");

    for (int i = 0; i < this.clusterWorkerThreads.length; i++) {
      this.clusterWorkerThreads[i] = new Thread(new ClusterCacheWorker(), "ApnFallbackManagerClusterWorker-" + i);
    }
  }

  public void schedule(Account account, Device device) throws RedisException {
    schedule(account, device, System.currentTimeMillis());
  }

  @VisibleForTesting
  void schedule(Account account, Device device, long timestamp) throws RedisException {
    try {
      sent.mark();
      insert(account, device, timestamp + (15 * 1000), (15 * 1000));
    } catch (io.lettuce.core.RedisException e) {
      throw new RedisException(e);
    }
  }

  public void cancel(Account account, Device device) throws RedisException {
    try {
      if (remove(account, device)) {
        delivered.mark();
      }
    } catch (JedisException | io.lettuce.core.RedisException e) {
      throw new RedisException(e);
    }
  }

  @Override
  public synchronized void start() {
    running.set(true);
    singletonWorkerThread.start();

    for (final Thread clusterWorkerThread : clusterWorkerThreads) {
      clusterWorkerThread.start();
    }
  }

  @Override
  public synchronized void stop() throws InterruptedException {
    running.set(false);
    singletonWorkerThread.join();

    for (final Thread clusterWorkerThread : clusterWorkerThreads) {
      clusterWorkerThread.join();
    }
  }

  private void sendNotification(final Account account, final Device device) {
    String apnId = device.getVoipApnId();

    if (apnId == null) {
      remove(account, device);
      return;
    }

    long deviceLastSeen = device.getLastSeen();

    if (deviceLastSeen < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(90)) {
      evicted.mark();
      remove(account, device);
      return;
    }

    apnSender.sendMessage(new ApnMessage(apnId, account.getNumber(), device.getId(), true, Optional.empty()));
    retry.mark();
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

  private boolean remove(Account account, Device device) {
    final boolean removedFromSingleton = removeFromSingleton(getSingletonEndpointKey(account, device));
    final boolean removedFromCluster = removeFromCluster(getClusterEndpointKey(account, device));

    return removedFromSingleton || removedFromCluster;
  }

  private boolean removeFromSingleton(String endpoint) {
    if (!SINGLETON_PENDING_NOTIFICATIONS_KEY.equals(endpoint)) {
      List<byte[]> keys = Arrays.asList(SINGLETON_PENDING_NOTIFICATIONS_KEY.getBytes(), endpoint.getBytes());
      List<byte[]> args = Collections.emptyList();

      return ((long)removeSingletonScript.execute(keys, args)) > 0;
    }

    return false;
  }

  private boolean removeFromCluster(final String endpoint) {
    final long removed = (long)removeClusterScript.execute(List.of(getClusterPendingNotificationQueueKey(endpoint), endpoint),
                                                           Collections.emptyList());

    return removed > 0;
  }

  @SuppressWarnings("unchecked")
  private List<String> getPendingDestinationsFromSingletonCache(final int limit) {
    List<byte[]> keys = List.of(SINGLETON_PENDING_NOTIFICATIONS_KEY.getBytes());
    List<byte[]> args = List.of(String.valueOf(System.currentTimeMillis()).getBytes(), String.valueOf(limit).getBytes());

    return ((List<byte[]>) getSingletonScript.execute(keys, args))
            .stream()
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  List<String> getPendingDestinationsFromClusterCache(final int slot, final int limit) {
    return (List<String>)getClusterScript.execute(List.of(getClusterPendingNotificationQueueKey(slot)),
                                                  List.of(String.valueOf(System.currentTimeMillis()), String.valueOf(limit)));
  }

  private void insert(final Account account, final Device device, final long timestamp, final long interval) {
    final String endpoint = getClusterEndpointKey(account, device);

    insertClusterScript.execute(List.of(getClusterPendingNotificationQueueKey(endpoint), endpoint),
                                List.of(String.valueOf(timestamp),
                                        String.valueOf(interval),
                                        account.getUuid().toString(),
                                        String.valueOf(device.getId())));
  }

  private String getSingletonEndpointKey(final Account account, final Device device) {
    return "apn_device::" + account.getNumber() + "::" + device.getId();
  }

  @VisibleForTesting
  String getClusterEndpointKey(final Account account, final Device device) {
    return "apn_device::{" + account.getUuid() + "::" + device.getId() + "}";
  }

  private String getClusterPendingNotificationQueueKey(final String endpoint) {
    return getClusterPendingNotificationQueueKey(SlotHash.getSlot(endpoint));
  }

  private String getClusterPendingNotificationQueueKey(final int slot) {
    return SINGLETON_PENDING_NOTIFICATIONS_KEY + "::{" + RedisClusterUtil.getMinimalHashTag(slot) + "}";
  }

  private int getNextSlot() {
    return (int)(cluster.withCluster(connection -> connection.sync().incr(NEXT_SLOT_TO_PERSIST_KEY)) % SlotHash.SLOT_COUNT);
  }

  private static class VoipRatioGauge extends RatioGauge {

    private final Meter success;
    private final Meter attempts;

    private VoipRatioGauge(Meter success, Meter attempts) {
      this.success  = success;
      this.attempts = attempts;
    }

    @Override
    protected Ratio getRatio() {
      return RatioGauge.Ratio.of(success.getFiveMinuteRate(), attempts.getFiveMinuteRate());
    }
  }

}
