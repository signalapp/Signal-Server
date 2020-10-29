/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.push.RetryingApnsClient.ApnResult;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Constants;

import javax.annotation.Nullable;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;

public class APNSender implements Managed {

  private final Logger logger = LoggerFactory.getLogger(APNSender.class);

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Meter unregisteredEventStale  = metricRegistry.meter(name(APNSender.class, "unregistered_event_stale"));
  private static final Meter unregisteredEventFresh  = metricRegistry.meter(name(APNSender.class, "unregistered_event_fresh"));

  private ApnFallbackManager fallbackManager;

  private final ExecutorService    executor;
  private final AccountsManager    accountsManager;
  private final String             bundleId;
  private final boolean            sandbox;
  private final RetryingApnsClient apnsClient;

  public APNSender(ExecutorService executor, AccountsManager accountsManager, ApnConfiguration configuration)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException
  {
    this.executor        = executor;
    this.accountsManager = accountsManager;
    this.bundleId        = configuration.getBundleId();
    this.sandbox         = configuration.isSandboxEnabled();
    this.apnsClient      = new RetryingApnsClient(configuration.getSigningKey(),
                                                  configuration.getTeamId(),
                                                  configuration.getKeyId(),
                                                  sandbox);
  }

  @VisibleForTesting
  public APNSender(ExecutorService executor, AccountsManager accountsManager, RetryingApnsClient apnsClient, String bundleId, boolean sandbox) {
    this.executor        = executor;
    this.accountsManager = accountsManager;
    this.apnsClient      = apnsClient;
    this.sandbox         = sandbox;
    this.bundleId        = bundleId;
  }

  public ListenableFuture<ApnResult> sendMessage(final ApnMessage message) {
    String topic = bundleId;

    if (message.isVoip()) {
      topic = topic + ".voip";
    }
    
    ListenableFuture<ApnResult> future = apnsClient.send(message.getApnId(), topic,
                                                         message.getMessage(),
                                                         Instant.ofEpochMilli(message.getExpirationTime()),
                                                         message.isVoip());

    Futures.addCallback(future, new FutureCallback<ApnResult>() {
      @Override
      public void onSuccess(@Nullable ApnResult result) {
        if (message.getChallengeData().isPresent()) return;

        if (result == null) {
          logger.warn("*** RECEIVED NULL APN RESULT ***");
        } else if (result.getStatus() == ApnResult.Status.NO_SUCH_USER) {
          handleUnregisteredUser(message.getApnId(), message.getNumber(), message.getDeviceId());
        } else if (result.getStatus() == ApnResult.Status.GENERIC_FAILURE) {
          logger.warn("*** Got APN generic failure: " + result.getReason() + ", " + message.getNumber());
        }
      }

      @Override
      public void onFailure(@Nullable Throwable t) {
        logger.warn("Got fatal APNS exception", t);
      }
    }, executor);

    return future;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
    this.apnsClient.disconnect();
  }

  public void setApnFallbackManager(ApnFallbackManager fallbackManager) {
    this.fallbackManager = fallbackManager;
  }

  private void handleUnregisteredUser(String registrationId, String number, long deviceId) {
//    logger.info("Got APN Unregistered: " + number + "," + deviceId);

    Optional<Account> account = accountsManager.get(number);

    if (!account.isPresent()) {
      logger.info("No account found: " + number);
      unregisteredEventStale.mark();
      return;
    }

    Optional<Device> device = account.get().getDevice(deviceId);

    if (!device.isPresent()) {
      logger.info("No device found: " + number);
      unregisteredEventStale.mark();
      return;
    }

    if (!registrationId.equals(device.get().getApnId()) &&
        !registrationId.equals(device.get().getVoipApnId()))
    {
      logger.info("Registration ID does not match: " + registrationId + ", " + device.get().getApnId() + ", " + device.get().getVoipApnId());
      unregisteredEventStale.mark();
      return;
    }

//    if (registrationId.equals(device.get().getApnId())) {
//      logger.info("APN Unregister APN ID matches! " + number + ", " + deviceId);
//    } else if (registrationId.equals(device.get().getVoipApnId())) {
//      logger.info("APN Unregister VoIP ID matches! " + number + ", " + deviceId);
//    }

    long tokenTimestamp = device.get().getPushTimestamp();

    if (tokenTimestamp != 0 && System.currentTimeMillis() < tokenTimestamp + TimeUnit.SECONDS.toMillis(10))
    {
      logger.info("APN Unregister push timestamp is more recent: " + tokenTimestamp + ", " + number);
      unregisteredEventStale.mark();
      return;
    }

//    logger.info("APN Unregister timestamp matches: " + device.get().getApnId() + ", " + device.get().getVoipApnId());
//    device.get().setApnId(null);
//    device.get().setVoipApnId(null);
//    device.get().setFetchesMessages(false);
//    accountsManager.update(account.get());

//    if (fallbackManager != null) {
//      fallbackManager.cancel(new WebsocketAddress(number, deviceId));
//    }

    if (fallbackManager != null) {
      RedisOperation.unchecked(() -> fallbackManager.cancel(account.get(), device.get()));
      unregisteredEventFresh.mark();
    }
  }
}
