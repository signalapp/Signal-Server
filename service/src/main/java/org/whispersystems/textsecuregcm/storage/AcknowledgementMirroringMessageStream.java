/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.reactivestreams.Subscription;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.BaseSubscriber;

/// A temporary message stream that can mirror message acknowledgements (deletion requests) to FoundationDB
public class AcknowledgementMirroringMessageStream implements MessageStream {

  private final RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private final FoundationDbMessageStream foundationDbMessageStream;

  private final Set<UUID> messagesPendingAcknowledgement = new HashSet<>();

  private static final int HIGH_PENDING_ACKNOWLEDGEMENT_COUNT_WARNING_THRESHOLD = 1024;

  @VisibleForTesting
  static final int MAX_PENDING_ACKNOWLEDGEMENTS = 8192;

  @VisibleForTesting
  static final int FOUNDATIONDB_REQUEST_SIZE = 100;

  private static final Counter HIGH_PENDING_ACKNOWLEDGEMENT_COUNT_WARNING_COUNTER =
      Metrics.counter(MetricsUtil.name(AcknowledgementMirroringMessageStream.class, "highPendingAcknowledgementCount"));

  private static final Counter MAX_PENDING_ACKNOWLEDGEMENT_LIMIT_BREACHED_COUNTER =
      Metrics.counter(MetricsUtil.name(AcknowledgementMirroringMessageStream.class, "maxPendingAcknowledgementLimitBreached"));

  private static class FoundationDbSubscriber extends BaseSubscriber<MessageStreamEntry.Envelope> {

    private final BiConsumer<UUID, Long> mirroredMessageHandler;

    private long mirroredEntriesDelivered = 0;
    private long requested = 0;

    private FoundationDbSubscriber(final BiConsumer<UUID, Long> mirroredMessageHandler) {
      this.mirroredMessageHandler = mirroredMessageHandler;
    }

    public void handleRedisDynamoDbMessageStreamEntry(final MessageStreamEntry messageStreamEntry) {
      final boolean isMirroredMessage = switch (messageStreamEntry) {
        case MessageStreamEntry.Envelope envelopeEntry ->
            UUIDUtil.fromByteString(envelopeEntry.message().getServerGuid()).version() == 8;

        case MessageStreamEntry.QueueEmpty _ -> false;
      };

      if (!isMirroredMessage) {
        return;
      }

      final boolean requestMore;

      synchronized (this) {
        requestMore = ++mirroredEntriesDelivered > requested;

        if (requestMore) {
          requested += FOUNDATIONDB_REQUEST_SIZE;
        }
      }

      if (requestMore) {
        request(FOUNDATIONDB_REQUEST_SIZE);
      }
    }

    @Override
    protected void hookOnSubscribe(final Subscription subscription) {
      // The base `hookOnSubscribe` requests `Long.MAX_VALUE` elements, and that is something we're explicitly trying
      // to avoid with this subscriber
    }

    @Override
    protected void hookOnNext(final MessageStreamEntry.Envelope envelope) {
      mirroredMessageHandler.accept(
          UUIDUtil.fromByteString(envelope.message().getServerGuid()), envelope.message().getServerTimestamp());
    }
  }

  public AcknowledgementMirroringMessageStream(final RedisDynamoDbMessageStream redisDynamoDbMessageStream,
                                               final FoundationDbMessageStream foundationDbMessageStream) {

    this.redisDynamoDbMessageStream = redisDynamoDbMessageStream;
    this.foundationDbMessageStream = foundationDbMessageStream;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    final FoundationDbSubscriber subscriber = new FoundationDbSubscriber(this::handleMirroredMessageAcknowledged);

    JdkFlowAdapter.flowPublisherToFlux(foundationDbMessageStream.getMessages())
        .filter(messageStreamEntry -> messageStreamEntry instanceof MessageStreamEntry.Envelope)
        .cast(MessageStreamEntry.Envelope.class)
        .subscribe(subscriber);

    return JdkFlowAdapter.publisherToFlowPublisher(
        JdkFlowAdapter.flowPublisherToFlux(redisDynamoDbMessageStream.getMessages())
            // Mirror demand and termination signals to the FoundationDB message stream
            .doOnNext(subscriber::handleRedisDynamoDbMessageStreamEntry)
            .doFinally(_ -> subscriber.dispose()));
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final UUID messageGuid, final long serverTimestamp) {
    // All messages stored in FoundationDB use version 8 UUIDs; if a message has a version 4 UUID, then it only exists
    // in Redis/DynamoDB
    if (messageGuid.version() == 8) {
      handleMirroredMessageAcknowledged(messageGuid, serverTimestamp);
    }

    return redisDynamoDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp);
  }

  private void handleMirroredMessageAcknowledged(final UUID messageGuid, final long serverTimestamp) {
    // Before we can acknowledge and delete the message in FoundationDB, two things need to happen:
    //
    // 1. The client needs to acknowledge the message on the Redis/DDB stream.
    // 2. The message needs to pass through the FoundationDB message stream pipeline because it needs to be registered
    //    in the acknowledged message buffer machinery before it can be deleted.
    //
    // We can't guarantee the order in which these two things happen, so we use a synchronized set to coordinate and
    // determine when both have happened.
    synchronized (messagesPendingAcknowledgement) {
      if (messagesPendingAcknowledgement.remove(messageGuid)) {
        // One of the two streams had already acknowledged this message, and this is the second acknowledgement. Now we
        // can pass the acknowledgement along to FoundationDB.
        foundationDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp);
      } else {
        // Either this message came from FoundationDB and got auto-acknowledged before getting explicitly acknowledged
        // on the Redis/DynamoDB stream or the message got explicitly acknowledged on the Redis/DynamoDB stream before
        // passing through the FoundationDB stream. Either way, wait for both streams to have done their part.
        if (messagesPendingAcknowledgement.add(messageGuid)) {
          if (messagesPendingAcknowledgement.size() == HIGH_PENDING_ACKNOWLEDGEMENT_COUNT_WARNING_THRESHOLD) {
            HIGH_PENDING_ACKNOWLEDGEMENT_COUNT_WARNING_COUNTER.increment();
          }

          if (messagesPendingAcknowledgement.size() > MAX_PENDING_ACKNOWLEDGEMENTS) {
            MAX_PENDING_ACKNOWLEDGEMENT_LIMIT_BREACHED_COUNTER.increment();
            throw new IllegalStateException("Too many pending acknowledgements");
          }
        }
      }
    }
  }
}
