/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.adapter.JdkFlowAdapter;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

/// A [MessageStream] implementation that produces message from a joint DynamoDB/Redis message store.
public class RedisDynamoDbMessageStream implements MessageStream {

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;

  private final UUID accountIdentifier;
  private final Device device;

  private final RedisDynamoDbMessagePublisher messagePublisher;

  private static final String MESSAGE_READ_COUNTER_NAME =
      name(RedisDynamoDbMessageStream.class, "messagesRead");

  private static final String MESSAGE_ACKNOWLEDGED_COUNTER_NAME =
      name(RedisDynamoDbMessageStream.class, "messagesAcknowledged");

  private static final String UUID_VERSION_TAG = "uuidVersion";

  public RedisDynamoDbMessageStream(final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final RedisMessageAvailabilityManager redisMessageAvailabilityManager,
      final UUID accountIdentifier,
      final Device device) {

    this(messagesDynamoDb, messagesCache, accountIdentifier, device, new RedisDynamoDbMessagePublisher(messagesDynamoDb,
        messagesCache,
        redisMessageAvailabilityManager,
        accountIdentifier,
        device));
  }

  @VisibleForTesting
  RedisDynamoDbMessageStream(final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final UUID accountIdentifier,
      final Device device,
      final RedisDynamoDbMessagePublisher messagePublisher) {

    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.accountIdentifier = accountIdentifier;
    this.device = device;
    this.messagePublisher = messagePublisher;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return JdkFlowAdapter.publisherToFlowPublisher(JdkFlowAdapter.flowPublisherToFlux(messagePublisher)
        .doOnNext(messageStreamEntry -> {
          if (messageStreamEntry instanceof MessageStreamEntry.Envelope(org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope message)) {
            final UUID messageGuid = UUIDUtil.fromByteString(message.getServerGuid());

            Metrics.counter(MESSAGE_READ_COUNTER_NAME, UUID_VERSION_TAG, String.valueOf(messageGuid.version()))
                .increment();
          }
        }));
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final UUID messageGuid, final long serverTimestamp) {
    Metrics.counter(MESSAGE_ACKNOWLEDGED_COUNTER_NAME, UUID_VERSION_TAG, String.valueOf(messageGuid.version()))
        .increment();

    return messagesCache.remove(accountIdentifier, device.getId(), messageGuid)
        .thenCompose(removed -> removed.map(_ -> CompletableFuture.<Void>completedFuture(null))
            .orElseGet(() ->
                messagesDynamoDb.deleteMessage(accountIdentifier, device, messageGuid, serverTimestamp)
                    .thenRun(Util.NOOP)))
        .whenComplete((_, _) -> messagePublisher.handleMessageAcknowledged());
  }
}
