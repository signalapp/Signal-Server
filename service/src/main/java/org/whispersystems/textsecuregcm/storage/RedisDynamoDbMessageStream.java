/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import org.whispersystems.textsecuregcm.util.Util;

/// A [MessageStream] implementation that produces message from a joint DynamoDB/Redis message store.
public class RedisDynamoDbMessageStream implements MessageStream {

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;

  private final UUID accountIdentifier;
  private final Device device;

  private final RedisDynamoDbMessagePublisher messagePublisher;

  public RedisDynamoDbMessageStream(final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final RedisMessageAvailabilityManager redisMessageAvailabilityManager,
      final UUID accountIdentifier,
      final Device device) {

    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.accountIdentifier = accountIdentifier;
    this.device = device;

    this.messagePublisher = new RedisDynamoDbMessagePublisher(messagesDynamoDb,
        messagesCache,
        redisMessageAvailabilityManager,
        accountIdentifier,
        device);
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return messagePublisher;
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final MessageProtos.Envelope message) {
    final UUID guid = UUID.fromString(message.getServerGuid());

    return messagesCache.remove(accountIdentifier, device.getId(), guid)
        .thenCompose(removed -> removed.map(_ -> CompletableFuture.<Void>completedFuture(null))
            .orElseGet(() ->
                messagesDynamoDb.deleteMessage(accountIdentifier, device, guid, message.getServerTimestamp())
                    .thenRun(Util.NOOP)));
  }
}
