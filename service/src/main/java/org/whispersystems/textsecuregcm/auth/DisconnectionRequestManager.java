/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

/**
 * A disconnection request manager broadcasts and dispatches requests for servers to close authenticated connections
 * from specific clients.
 *
 * @see DisconnectionRequestListener
 */
public class DisconnectionRequestManager extends RedisPubSubAdapter<byte[], byte[]> implements Managed {

  private final FaultTolerantRedisClient pubSubClient;
  private final GrpcClientConnectionManager grpcClientConnectionManager;
  private final Executor listenerEventExecutor;
  private final ScheduledExecutorService retryExecutor;

  private static final String RETRY_NAME = ResilienceUtil.name(DisconnectionRequestManager.class);

  private static final Duration SUBSCRIBE_RETRY_DELAY = Duration.ofSeconds(5);

  private final Map<AccountIdentifierAndDeviceId, List<DisconnectionRequestListener>> listeners =
      new ConcurrentHashMap<>();

  @Nullable
  private FaultTolerantPubSubConnection<byte[], byte[]> pubSubConnection;

  private static final byte[] DISCONNECTION_REQUEST_CHANNEL = "disconnection_requests".getBytes(StandardCharsets.UTF_8);

  private static final Counter DISCONNECTION_REQUESTS_SENT_COUNTER =
      Metrics.counter(MetricsUtil.name(DisconnectionRequestManager.class, "requestsSent"));

  private static final Counter DISCONNECTION_REQUESTS_RECEIVED_COUNTER =
      Metrics.counter(MetricsUtil.name(DisconnectionRequestManager.class, "requestsReceived"));

  private static final Logger logger = LoggerFactory.getLogger(DisconnectionRequestManager.class);

  private record AccountIdentifierAndDeviceId(UUID accountIdentifier, byte deviceId) {}

  public DisconnectionRequestManager(final FaultTolerantRedisClient pubSubClient,
      final GrpcClientConnectionManager grpcClientConnectionManager,
      final Executor listenerEventExecutor,
      final ScheduledExecutorService retryExecutor) {

    this.pubSubClient = pubSubClient;
    this.grpcClientConnectionManager = grpcClientConnectionManager;
    this.listenerEventExecutor = listenerEventExecutor;
    this.retryExecutor = retryExecutor;
  }

  @Override
  public synchronized void start() {
    this.pubSubConnection = pubSubClient.createBinaryPubSubConnection();
    this.pubSubConnection.usePubSubConnection(connection -> {
      connection.addListener(this);

      boolean subscribed = false;

      // Loop indefinitely until we establish a subscription. We don't want to fail immediately if there's a temporary
      // Redis connectivity issue, since that would derail the whole startup process and likely lead to unnecessary pod
      // churn, which might make things worse. If we never establish a connection, readiness probes will eventually fail
      // and terminate the pods.
      do {
        try {
          connection.sync().subscribe(DISCONNECTION_REQUEST_CHANNEL);
          subscribed = true;
        } catch (final RedisCommandTimeoutException e) {
          try {
            Thread.sleep(SUBSCRIBE_RETRY_DELAY);
          } catch (final InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      } while (!subscribed);
    });
  }

  @Override
  public synchronized void stop() {
    if (pubSubConnection != null) {
      pubSubConnection.usePubSubConnection(connection -> {
        connection.removeListener(this);
        connection.close();
      });
    }

    pubSubConnection = null;
  }

  /**
   * Adds a listener for disconnection requests for a specific authenticated device.
   *
   * @param accountIdentifier TODO
   * @param deviceId TODO
   * @param listener the listener to register
   */
  public void addListener(final UUID accountIdentifier, final byte deviceId, final DisconnectionRequestListener listener) {
    listeners.compute(new AccountIdentifierAndDeviceId(accountIdentifier, deviceId), (_, existingListeners) -> {
      final List<DisconnectionRequestListener> listeners =
          existingListeners == null ? new ArrayList<>() : existingListeners;

      listeners.add(listener);

      return listeners;
    });
  }

  /**
   * Removes a listener for disconnection requests for a specific authenticated device.
   *
   * @param accountIdentifier TODO
   * @param deviceId TODO
   * @param listener the listener to remove
   */
  public void removeListener(final UUID accountIdentifier, final byte deviceId, final DisconnectionRequestListener listener) {
    listeners.computeIfPresent(new AccountIdentifierAndDeviceId(accountIdentifier, deviceId), (_, existingListeners) -> {
      existingListeners.remove(listener);

      return existingListeners.isEmpty() ? null : existingListeners;
    });
  }

  @VisibleForTesting
  List<DisconnectionRequestListener> getListeners(final UUID accountIdentifier, final byte deviceId) {
    return listeners.getOrDefault(new AccountIdentifierAndDeviceId(accountIdentifier, deviceId), Collections.emptyList());
  }

  /**
   * Broadcasts a request to close all connections associated with the given account identifier to all servers.
   *
   * @param account the account for which to close connections
   *
   * @return a future that completes when the request has been broadcast
   */
  public CompletionStage<Void> requestDisconnection(final Account account) {
    return requestDisconnection(account.getIdentifier(IdentityType.ACI),
        account.getDevices().stream().map(Device::getId).toList());
  }

  /**
   * Broadcasts a request to close connections associated with the given account identifier and device IDs to all
   * servers.
   *
   * @param accountIdentifier the account for which to close connections
   * @param deviceIds the device IDs for which to close connections
   *
   * @return a future that completes when the request has been broadcast
   */
  public CompletionStage<Void> requestDisconnection(final UUID accountIdentifier, final Collection<Byte> deviceIds) {
    final DisconnectionRequest disconnectionRequest = DisconnectionRequest.newBuilder()
        .setAccountIdentifier(UUIDUtil.toByteString(accountIdentifier))
        .addAllDeviceIds(deviceIds.stream().mapToInt(Byte::intValue).boxed().toList())
        .build();

    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeCompletionStage(retryExecutor, () -> pubSubClient.withBinaryConnection(connection ->
                connection.async().publish(DISCONNECTION_REQUEST_CHANNEL, disconnectionRequest.toByteArray()))
            .toCompletableFuture())
        .thenRun(DISCONNECTION_REQUESTS_SENT_COUNTER::increment);
  }

  @Override
  public void message(final byte[] channel, final byte[] message) {
    final UUID accountIdentifier;
    final List<Byte> deviceIds;

    try {
      final DisconnectionRequest disconnectionRequest = DisconnectionRequest.parseFrom(message);
      DISCONNECTION_REQUESTS_RECEIVED_COUNTER.increment();

      accountIdentifier = UUIDUtil.fromByteString(disconnectionRequest.getAccountIdentifier());
      deviceIds = disconnectionRequest.getDeviceIdsList().stream()
          .map(deviceIdInt -> {
            if (deviceIdInt == null || deviceIdInt < Device.PRIMARY_ID || deviceIdInt > Byte.MAX_VALUE) {
              throw new IllegalArgumentException("Invalid device ID: " + deviceIdInt);
            }

            return deviceIdInt.byteValue();
          })
          .toList();
    } catch (final InvalidProtocolBufferException e) {
      logger.error("Could not parse disconnection request protobuf", e);
      return;
    } catch (final IllegalArgumentException e) {
      logger.error("Could not parse part of disconnection request", e);
      return;
    }

    deviceIds.forEach(deviceId -> {
      grpcClientConnectionManager.closeConnection(new AuthenticatedDevice(accountIdentifier, deviceId));

      listeners.getOrDefault(new AccountIdentifierAndDeviceId(accountIdentifier, deviceId), Collections.emptyList())
          .forEach(listener -> listenerEventExecutor.execute(() -> {
            try {
              listener.handleDisconnectionRequest();
            } catch (final Exception e) {
              logger.warn("Listener failed to handle disconnection request", e);
            }
          }));
    });
  }
}
