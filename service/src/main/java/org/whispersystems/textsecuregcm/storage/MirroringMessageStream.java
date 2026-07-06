/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.Disposable;

/// A temporary message stream that can mirror message acknowledgements (deletion requests) to FoundationDB
public class MirroringMessageStream implements MessageStream {

  private final RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private final FoundationDbMessageStore foundationDbMessageStore;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final AciServiceIdentifier accountIdentifier;
  private final byte deviceId;

  private static final Logger logger = LoggerFactory.getLogger(MirroringMessageStream.class);

  public MirroringMessageStream(final RedisDynamoDbMessageStream redisDynamoDbMessageStream,
      final FoundationDbMessageStore foundationDbMessageStore,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final UUID accountIdentifier,
      final byte deviceId) {

    this.redisDynamoDbMessageStream = redisDynamoDbMessageStream;
    this.foundationDbMessageStore = foundationDbMessageStore;
    this.experimentEnrollmentManager = experimentEnrollmentManager;

    this.accountIdentifier = new AciServiceIdentifier(accountIdentifier);
    this.deviceId = deviceId;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    if (experimentEnrollmentManager.isEnrolled(accountIdentifier.uuid(), MessagesManager.MIRROR_READS_EXPERIMENT_NAME)) {
      final Disposable foundationDbDisposable =
          foundationDbMessageStore.getMessages(accountIdentifier, deviceId).getFiniteMessageStream()
              .limitRate(100)
              .subscribe();

      return JdkFlowAdapter.publisherToFlowPublisher(
          JdkFlowAdapter.flowPublisherToFlux(redisDynamoDbMessageStream.getMessages())
              .doFinally(_ -> {
                try {
                  foundationDbDisposable.dispose();
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

      foundationDbMessageStore.delete(accountIdentifier, deviceId, messageGuid)
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
