/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

/**
 * A disconnection request manager broadcasts and dispatches requests for servers to close authenticated connections
 * from specific clients.
 *
 * @see DisconnectionRequestListener
 */
public class DisconnectionRequestManager extends RedisPubSubAdapter<byte[], byte[]> implements Managed {

  private final FaultTolerantRedisClient pubSubClient;
  private final Executor listenerEventExecutor;

  // We expect just a couple listeners to get added at startup time and not at all at steady-state. There are several
  // reasonable ways to model this, but a copy-on-write list gives us good flexibility with minimal performance cost.
  private final List<DisconnectionRequestListener> listeners = new CopyOnWriteArrayList<>();

  @Nullable
  private FaultTolerantPubSubConnection<byte[], byte[]> pubSubConnection;

  private static final byte[] DISCONNECTION_REQUEST_CHANNEL = "disconnection_requests".getBytes(StandardCharsets.UTF_8);

  private static final Counter DISCONNECTION_REQUESTS_SENT_COUNTER =
      Metrics.counter(MetricsUtil.name(DisconnectionRequestManager.class, "requestsSent"));

  private static final Counter DISCONNECTION_REQUESTS_RECEIVED_COUNTER =
      Metrics.counter(MetricsUtil.name(DisconnectionRequestManager.class, "requestsReceived"));

  private static final Logger logger = LoggerFactory.getLogger(DisconnectionRequestManager.class);

  public DisconnectionRequestManager(final FaultTolerantRedisClient pubSubClient,
      final Executor listenerEventExecutor) {

    this.pubSubClient = pubSubClient;
    this.listenerEventExecutor = listenerEventExecutor;
  }

  @Override
  public synchronized void start() {
    this.pubSubConnection = pubSubClient.createBinaryPubSubConnection();
    this.pubSubConnection.usePubSubConnection(connection -> {
      connection.addListener(this);
      connection.sync().subscribe(DISCONNECTION_REQUEST_CHANNEL);
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
   * Adds a listener for disconnection requests. Listeners will receive all broadcast disconnection requests regardless
   * of whether the device in connection is connected to this server.
   *
   * @param listener the listener to register
   */
  public void addListener(final DisconnectionRequestListener listener) {
    listeners.add(listener);
  }

  /**
   * Broadcasts a request to close all connections associated with the given account identifier to all servers.
   *
   * @param accountIdentifier the account for which to close connections
   *
   * @return a future that completes when the request has been broadcast
   */
  public CompletionStage<Void> requestDisconnection(final UUID accountIdentifier) {
    return requestDisconnection(accountIdentifier, Collections.emptyList());
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

    return pubSubClient.withBinaryConnection(connection ->
            connection.async().publish(DISCONNECTION_REQUEST_CHANNEL, disconnectionRequest.toByteArray()))
        .toCompletableFuture()
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
      deviceIds = disconnectionRequest.getDeviceIdsCount() > 0
          ? disconnectionRequest.getDeviceIdsList().stream()
          .map(deviceIdInt -> {
            if (deviceIdInt == null || deviceIdInt < Device.PRIMARY_ID || deviceIdInt > Byte.MAX_VALUE) {
              throw new IllegalArgumentException("Invalid device ID: " + deviceIdInt);
            }

            return deviceIdInt.byteValue();
          })
          .toList()
          : Device.ALL_POSSIBLE_DEVICE_IDS;
    } catch (final InvalidProtocolBufferException e) {
      logger.error("Could not parse disconnection request protobuf", e);
      return;
    } catch (final IllegalArgumentException e) {
      logger.error("Could not parse part of disconnection request", e);
      return;
    }

    for (final DisconnectionRequestListener listener : listeners) {
      try {
        listenerEventExecutor.execute(() -> listener.handleDisconnectionRequest(accountIdentifier, deviceIds));
      } catch (final Exception e) {
        logger.warn("Listener failed to handle disconnection request", e);
      }
    }
  }
}
