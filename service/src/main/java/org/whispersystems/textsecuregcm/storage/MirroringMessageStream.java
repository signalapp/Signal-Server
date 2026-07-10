/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.BaseSubscriber;

/// A temporary message stream that can mirror message acknowledgements (deletion requests) to FoundationDB
public class MirroringMessageStream implements MessageStream {

  private final RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private final FoundationDbMessageStream foundationDbMessageStream;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final AciServiceIdentifier accountIdentifier;
  private final byte deviceId;

  private static final Logger logger = LoggerFactory.getLogger(MirroringMessageStream.class);

  private static class FoundationDbSubscriber extends BaseSubscriber<MessageStreamEntry> {

    public void handleRedisDynamoDbMessageStreamEntry(final MessageStreamEntry messageStreamEntry) {
      final boolean isMirroredMessage = switch (messageStreamEntry) {
        case MessageStreamEntry.Envelope envelopeEntry ->
            UUIDUtil.fromByteString(envelopeEntry.message().getServerGuid()).version() == 8;

        case MessageStreamEntry.QueueEmpty _ -> true;
      };

      if (isMirroredMessage) {
        request(1);
      }
    }

    @Override
    protected void hookOnSubscribe(final Subscription subscription) {
      // The base `hookOnSubscribe` requests `Long.MAX_VALUE` elements, and that is something we're explicitly trying
      // to avoid with this subscriber
    }
  }

  public MirroringMessageStream(final RedisDynamoDbMessageStream redisDynamoDbMessageStream,
      final FoundationDbMessageStream foundationDbMessageStream,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final UUID accountIdentifier,
      final byte deviceId) {

    this.redisDynamoDbMessageStream = redisDynamoDbMessageStream;
    this.foundationDbMessageStream = foundationDbMessageStream;
    this.experimentEnrollmentManager = experimentEnrollmentManager;

    this.accountIdentifier = new AciServiceIdentifier(accountIdentifier);
    this.deviceId = deviceId;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    final FoundationDbSubscriber subscriber = new FoundationDbSubscriber();

    if (experimentEnrollmentManager.isEnrolled(accountIdentifier.uuid(), MessagesManager.MIRROR_READS_EXPERIMENT_NAME)) {
      JdkFlowAdapter.flowPublisherToFlux(foundationDbMessageStream.getMessages())
          .subscribe(subscriber);

      return JdkFlowAdapter.publisherToFlowPublisher(
          JdkFlowAdapter.flowPublisherToFlux(redisDynamoDbMessageStream.getMessages())
              .doOnNext(subscriber::handleRedisDynamoDbMessageStreamEntry)
              .doFinally(_ -> {
                try {
                  subscriber.dispose();
                } catch (final Exception _) {
                }
              }));
    }

    return redisDynamoDbMessageStream.getMessages();
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final UUID messageGuid, final long serverTimestamp) {
    // All messages stored in FoundationDB use version 8 UUIDs; if a message has a version 4 UUID, then it only exists
    // in Redis/DynamoDB
    if (messageGuid.version() == 8 &&
        experimentEnrollmentManager.isEnrolled(accountIdentifier.uuid(), MessagesManager.MIRROR_DELETIONS_EXPERIMENT_NAME)) {

      foundationDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp)
          .whenComplete((_, throwable) -> {
            if (throwable != null) {
              logger.warn("Failed to delete message {}/{}/{} from FoundationDb", accountIdentifier.uuid(), deviceId,
                  messageGuid, throwable);
            }
          });
    }

    return redisDynamoDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp);
  }
}
