/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;

public class ProvisioningManager extends RedisPubSubAdapter<byte[], byte[]> implements Managed {

  private final FaultTolerantRedisClient pubSubClient;
  private final FaultTolerantPubSubConnection<byte[], byte[]> pubSubConnection;

  private final Map<String, Consumer<PubSubProtos.PubSubMessage>> listenersByProvisioningAddress =
      new ConcurrentHashMap<>();

  private static final String ACTIVE_LISTENERS_GAUGE_NAME = name(ProvisioningManager.class, "activeListeners");

  private static final String SEND_PROVISIONING_MESSAGE_COUNTER_NAME =
      name(ProvisioningManager.class, "sendProvisioningMessage");

  private static final String RECEIVE_PROVISIONING_MESSAGE_COUNTER_NAME =
      name(ProvisioningManager.class, "receiveProvisioningMessage");

  private static final Logger logger = LoggerFactory.getLogger(ProvisioningManager.class);

  public ProvisioningManager(final FaultTolerantRedisClient pubSubClient) {
    this.pubSubClient = pubSubClient;
    this.pubSubConnection = pubSubClient.createBinaryPubSubConnection();

    Metrics.gaugeMapSize(ACTIVE_LISTENERS_GAUGE_NAME, Tags.empty(), listenersByProvisioningAddress);
  }

  @Override
  public void start() throws Exception {
    pubSubConnection.usePubSubConnection(connection -> connection.addListener(this));
  }

  @Override
  public void stop() throws Exception {
    pubSubConnection.usePubSubConnection(connection -> connection.removeListener(this));
  }

  public void addListener(final String address, final Consumer<PubSubProtos.PubSubMessage> listener) {
    listenersByProvisioningAddress.put(address, listener);
    pubSubConnection.usePubSubConnection(connection -> connection.sync().subscribe(address.getBytes(StandardCharsets.UTF_8)));
  }

  public void removeListener(final String address) {
    RedisOperation.unchecked(() ->
        pubSubConnection.usePubSubConnection(connection -> connection.sync().unsubscribe(address.getBytes(StandardCharsets.UTF_8))));

    listenersByProvisioningAddress.remove(address);
  }

  public boolean sendProvisioningMessage(final String address, final byte[] body) {
    final PubSubProtos.PubSubMessage pubSubMessage = PubSubProtos.PubSubMessage.newBuilder()
        .setType(PubSubProtos.PubSubMessage.Type.DELIVER)
        .setContent(ByteString.copyFrom(body))
        .build();

    final boolean receiverPresent = pubSubClient.withBinaryConnection(connection ->
        connection.sync().publish(address.getBytes(StandardCharsets.UTF_8), pubSubMessage.toByteArray()) > 0);

    Metrics.counter(SEND_PROVISIONING_MESSAGE_COUNTER_NAME, "online", String.valueOf(receiverPresent)).increment();

    return receiverPresent;
  }

  @Override
  public void message(final byte[] channel, final byte[] message) {
    try {
      final String address = new String(channel, StandardCharsets.UTF_8);
      final PubSubProtos.PubSubMessage pubSubMessage = PubSubProtos.PubSubMessage.parseFrom(message);

      if (pubSubMessage.getType() == PubSubProtos.PubSubMessage.Type.DELIVER) {
        final Consumer<PubSubProtos.PubSubMessage> listener = listenersByProvisioningAddress.get(address);

        boolean listenerPresent = false;

        if (listener != null) {
          listenerPresent = true;
          listener.accept(pubSubMessage);
        }

        Metrics.counter(RECEIVE_PROVISIONING_MESSAGE_COUNTER_NAME, "listenerPresent", String.valueOf(listenerPresent)).increment();
      }
    } catch (final InvalidProtocolBufferException e) {
      logger.warn("Failed to parse pub/sub message", e);
    }
  }

  @Override
  public void unsubscribed(final byte[] channel, final long count) {
    listenersByProvisioningAddress.remove(new String(channel, StandardCharsets.UTF_8));
  }
}
