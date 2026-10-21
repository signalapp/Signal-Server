/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;

public class ConcatenatingMessageStream implements MessageStream {

  private final DynamoDbMessageStream dynamoDbMessageStream;
  private final FoundationDbMessageStream foundationDbMessageStream;

  private final AtomicBoolean shouldDeleteFromDynamoDb = new AtomicBoolean(false);

  private static final Counter CONFLICTING_CONSUMER_COUNTER =
      Metrics.counter(MetricsUtil.name(ConcatenatingMessageStream.class, "conflictingConsumer"));

  public ConcatenatingMessageStream(final DynamoDbMessageStream dynamoDbMessageStream,
      final FoundationDbMessageStream foundationDbMessageStream) {

    this.dynamoDbMessageStream = dynamoDbMessageStream;
    this.foundationDbMessageStream = foundationDbMessageStream;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return JdkFlowAdapter.publisherToFlowPublisher(Flux.concat(
        JdkFlowAdapter.flowPublisherToFlux(dynamoDbMessageStream.getMessages())
            .doOnNext(messageStreamEntry -> {
              if (messageStreamEntry instanceof MessageStreamEntry.Envelope) {
                shouldDeleteFromDynamoDb.set(true);
              }
            }),
        JdkFlowAdapter.flowPublisherToFlux(foundationDbMessageStream.getMessages()))
        .doOnError(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ConflictingMessageConsumerException) {
            CONFLICTING_CONSUMER_COUNTER.increment();
          }
        }));
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final UUID messageGuid, final long serverTimestamp) {
    return CompletableFuture.allOf(
        shouldDeleteFromDynamoDb.get()
            ? dynamoDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp)
            : CompletableFuture.completedFuture(null),
        messageGuid.version() == 8
            ? foundationDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp)
            : CompletableFuture.completedFuture(null));
  }
}
