/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.lifecycle.Managed;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.redis.RedisUriUtil;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.websocket.InvalidWebsocketAddressException;
import org.whispersystems.textsecuregcm.websocket.ProvisioningAddress;

public class ProvisioningManager extends RedisPubSubAdapter<byte[], byte[]> implements Managed {

  private final RedisClient redisClient;
  private final StatefulRedisPubSubConnection<byte[], byte[]> subscriptionConnection;
  private final StatefulRedisConnection<byte[], byte[]> publicationConnection;

  private final CircuitBreaker circuitBreaker;

  private final Map<ProvisioningAddress, Consumer<PubSubProtos.PubSubMessage>> listenersByProvisioningAddress =
      new ConcurrentHashMap<>();

  private static final String ACTIVE_LISTENERS_GAUGE_NAME = name(ProvisioningManager.class, "activeListeners");

  private static final String SEND_PROVISIONING_MESSAGE_COUNTER_NAME =
      name(ProvisioningManager.class, "sendProvisioningMessage");

  private static final String RECEIVE_PROVISIONING_MESSAGE_COUNTER_NAME =
      name(ProvisioningManager.class, "receiveProvisioningMessage");

  private static final Logger logger = LoggerFactory.getLogger(ProvisioningManager.class);

  public ProvisioningManager(final String redisUri,
      final ClientResources clientResources,
      final Duration timeout,
      final CircuitBreakerConfiguration circuitBreakerConfiguration) {

    this(RedisClient.create(clientResources, RedisUriUtil.createRedisUriWithTimeout(redisUri, timeout)), timeout,
        circuitBreakerConfiguration);
  }

  @VisibleForTesting
  ProvisioningManager(final RedisClient redisClient,
      final Duration timeout,
      final CircuitBreakerConfiguration circuitBreakerConfiguration) {

    this.redisClient = redisClient;
    this.redisClient.setDefaultTimeout(timeout);

    this.subscriptionConnection = redisClient.connectPubSub(new ByteArrayCodec());
    this.publicationConnection = redisClient.connect(new ByteArrayCodec());

    this.circuitBreaker = CircuitBreaker.of("pubsub-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());

    CircuitBreakerUtil.registerMetrics(circuitBreaker, ProvisioningManager.class);

    Metrics.gaugeMapSize(ACTIVE_LISTENERS_GAUGE_NAME, Tags.empty(), listenersByProvisioningAddress);
  }

  @Override
  public void start() throws Exception {
    subscriptionConnection.addListener(this);
  }

  @Override
  public void stop() throws Exception {
    subscriptionConnection.removeListener(this);

    subscriptionConnection.close();
    publicationConnection.close();

    redisClient.shutdown();
  }

  public void addListener(final ProvisioningAddress address, final Consumer<PubSubProtos.PubSubMessage> listener) {
    listenersByProvisioningAddress.put(address, listener);

    circuitBreaker.executeRunnable(
        () -> subscriptionConnection.sync().subscribe(address.serialize().getBytes(StandardCharsets.UTF_8)));
  }

  public void removeListener(final ProvisioningAddress address) {
    RedisOperation.unchecked(() -> circuitBreaker.executeRunnable(
        () -> subscriptionConnection.sync().unsubscribe(address.serialize().getBytes(StandardCharsets.UTF_8))));

    listenersByProvisioningAddress.remove(address);
  }

  public boolean sendProvisioningMessage(final ProvisioningAddress address, final byte[] body) {
    final PubSubProtos.PubSubMessage pubSubMessage = PubSubProtos.PubSubMessage.newBuilder()
        .setType(PubSubProtos.PubSubMessage.Type.DELIVER)
        .setContent(ByteString.copyFrom(body))
        .build();

    final boolean receiverPresent = circuitBreaker.executeSupplier(
        () -> publicationConnection.sync()
            .publish(address.serialize().getBytes(StandardCharsets.UTF_8), pubSubMessage.toByteArray()) > 0);

    Metrics.counter(SEND_PROVISIONING_MESSAGE_COUNTER_NAME, "online", String.valueOf(receiverPresent)).increment();

    return receiverPresent;
  }

  @Override
  public void message(final byte[] channel, final byte[] message) {
    try {
      final ProvisioningAddress address = new ProvisioningAddress(new String(channel, StandardCharsets.UTF_8));
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
    } catch (final InvalidWebsocketAddressException e) {
      logger.warn("Failed to parse provisioning address", e);
    } catch (final InvalidProtocolBufferException e) {
      logger.warn("Failed to parse pub/sub message", e);
    }
  }

  @Override
  public void unsubscribed(final byte[] channel, final long count) {
    try {
      listenersByProvisioningAddress.remove(new ProvisioningAddress(new String(channel)));
    } catch (final InvalidWebsocketAddressException e) {
      logger.warn("Failed to parse provisioning address for `unsubscribe` event", e);
    }
  }
}
