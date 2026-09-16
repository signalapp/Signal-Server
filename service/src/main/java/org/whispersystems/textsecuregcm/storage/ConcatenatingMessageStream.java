/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public class ConcatenatingMessageStream implements MessageStream {

  private final RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private final FoundationDbMessageStream foundationDbMessageStream;

  private static final Counter CONFLICTING_CONSUMER_COUNTER =
      Metrics.counter(MetricsUtil.name(ConcatenatingMessageStream.class, "conflictingConsumer"));

  public ConcatenatingMessageStream(final RedisDynamoDbMessageStream redisDynamoDbMessageStream,
      final FoundationDbMessageStream foundationDbMessageStream) {

    this.redisDynamoDbMessageStream = redisDynamoDbMessageStream;
    this.foundationDbMessageStream = foundationDbMessageStream;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return JdkFlowAdapter.publisherToFlowPublisher(Flux.concat(
        JdkFlowAdapter.flowPublisherToFlux(redisDynamoDbMessageStream.getMessages()),
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
        redisDynamoDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp),
        messageGuid.version() == 8
            ? foundationDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp)
            : CompletableFuture.completedFuture(null));
  }
}
