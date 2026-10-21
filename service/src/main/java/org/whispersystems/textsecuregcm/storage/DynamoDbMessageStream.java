/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;

/// A [MessageStream] implementation that produces messages from a DynamoDB message store.
public class DynamoDbMessageStream implements MessageStream {

  private final MessagesDynamoDb messagesDynamoDb;

  private final UUID accountIdentifier;
  private final Device device;

  private static final String MESSAGE_READ_COUNTER_NAME =
      name(DynamoDbMessageStream.class, "messagesRead");

  private static final String MESSAGE_ACKNOWLEDGED_COUNTER_NAME =
      name(DynamoDbMessageStream.class, "messagesAcknowledged");

  private static final String UUID_VERSION_TAG = "uuidVersion";

  public DynamoDbMessageStream(final MessagesDynamoDb messagesDynamoDb,
      final UUID accountIdentifier,
      final Device device) {

    this.messagesDynamoDb = messagesDynamoDb;

    this.accountIdentifier = accountIdentifier;
    this.device = device;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return JdkFlowAdapter.publisherToFlowPublisher(Flux.from(messagesDynamoDb.load(accountIdentifier, device, null))
        .doOnNext(envelope -> {
          final UUID messageGuid = UUIDUtil.fromByteString(envelope.getServerGuid());

          Metrics.counter(MESSAGE_READ_COUNTER_NAME, UUID_VERSION_TAG, String.valueOf(messageGuid.version()))
              .increment();
        })
        .map(MessageStreamEntry.Envelope::new));
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final UUID messageGuid, final long serverTimestamp) {
    Metrics.counter(MESSAGE_ACKNOWLEDGED_COUNTER_NAME, UUID_VERSION_TAG, String.valueOf(messageGuid.version()))
        .increment();

    return messagesDynamoDb.deleteMessage(accountIdentifier, device, messageGuid, serverTimestamp)
        .thenRun(Util.NOOP);
  }
}
