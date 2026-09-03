/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.protobuf.CodedOutputStream;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import jakarta.annotation.Nullable;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;

/// A temporary message stream that can mirror message acknowledgements (deletion requests) to FoundationDB
public class MirroringMessageStream implements MessageStream {

  private final RedisDynamoDbMessageStream redisDynamoDbMessageStream;
  private final FoundationDbMessageStream foundationDbMessageStream;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final AciServiceIdentifier accountIdentifier;
  private final byte deviceId;

  private static final Counter CONFLICTING_CONSUMER_COUNTER =
      Metrics.counter(MetricsUtil.name(MirroringMessageStream.class, "conflictingConsumer"));

  private static final Logger logger = LoggerFactory.getLogger(MirroringMessageStream.class);

  private static final Counter MESSAGE_MISMATCHES_COUNTER =
      Metrics.counter(MetricsUtil.name(MirroringMessageStream.class, "messageMismatches"));

  private static final String STREAM_AGREEMENTS = MetricsUtil.name(MirroringMessageStream.class, "streamAgreements");

  private static final String MISSING_MESSAGES_COUNTER =
      MetricsUtil.name(MirroringMessageStream.class, "missingMessages");

  private static final int MAX_WINDOW_SIZE = 1024;

  private final Map<UUID, HashCode> redisDynamoMessageWindow = new HashMap<>();
  private final Map<UUID, HashCode> foundationDbMessageWindow = new HashMap<>();

  private boolean stopAgreementVerification = false;

  @Nullable
  private Tags agreementFailureTags;

  private final Set<UUID> recentFoundationDbUuids = Collections.newSetFromMap(new LinkedHashMap<>() {
    @Override
    protected boolean removeEldestEntry(final Map.Entry<UUID, Boolean> eldest) {
      return size() > MAX_WINDOW_SIZE;
    }
  });

  private final Set<UUID> recentRedisDynamoEphemeralUuids = Collections.newSetFromMap(new LinkedHashMap<>() {
    @Override
    protected boolean removeEldestEntry(final Map.Entry<UUID, Boolean> eldest) {
      return size() > MAX_WINDOW_SIZE;
    }
  });

  private enum Stream {
    REDIS_DYNAMO("redisDynamo"),
    FOUNDATION_DB("foundationDb");

    private final String name;

    Stream(final String name) {
      this.name = name;
    }
  }

  private final FoundationDbSubscriber subscriber = new FoundationDbSubscriber();

  private class FoundationDbSubscriber extends BaseSubscriber<MessageStreamEntry> {

    public void handleRedisDynamoDbMessageStreamEntry(final MessageStreamEntry messageStreamEntry) {
      final boolean isMirroredMessage = switch (messageStreamEntry) {
        case final MessageStreamEntry.Envelope envelopeEntry -> {
          final boolean isUUIDv8 = UUIDUtil.fromByteString(envelopeEntry.message().getServerGuid()).version() == 8;
          if (isUUIDv8) {
            verifyMessageAgreement(envelopeEntry, Stream.REDIS_DYNAMO);
          }
          yield isUUIDv8;
        }

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

    @Override
    protected void hookOnError(final Throwable throwable) {
      switch (throwable) {
        case ConflictingMessageConsumerException _ -> CONFLICTING_CONSUMER_COUNTER.increment();
        default -> super.hookOnError(throwable);
      }
    }

    protected void hookOnNext(final MessageStreamEntry messageStreamEntry) {
      if (messageStreamEntry instanceof final MessageStreamEntry.Envelope envelope) {
        verifyMessageAgreement(envelope, Stream.FOUNDATION_DB);
      }
    }

    @Override
    protected void hookFinally(final SignalType type) {
      super.hookFinally(type);
      publishStreamAgreementMetrics();
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

    if (experimentEnrollmentManager.isEnrolled(accountIdentifier.uuid(),
        MessagesManager.MIRROR_READS_EXPERIMENT_NAME)) {
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
        experimentEnrollmentManager.isEnrolled(accountIdentifier.uuid(),
            MessagesManager.MIRROR_DELETIONS_EXPERIMENT_NAME)) {

      if (experimentEnrollmentManager.isEnrolled(accountIdentifier.uuid(), MessagesManager.MIRROR_READS_EXPERIMENT_NAME)
          &&
          messageNotDeliveredOnFoundationDbStream(messageGuid)) {
        foundationDbMessageStream.acknowledgeAndGetMessage(messageGuid)
            .thenAccept(deleteMessage -> {
              if (deleteMessage.isEmpty()) {
                handleMissingFoundationDbMessage(messageGuid);
                return;
              }
              verifyMessageAgreement(deleteMessage.get(), Stream.FOUNDATION_DB);
            })
            .whenComplete((_, throwable) -> {
              if (throwable != null) {
                logger.warn("Failed to delete message {}/{}/{} from FoundationDb", accountIdentifier.uuid(), deviceId,
                    messageGuid, throwable);
              }
            });
      } else {
        foundationDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp)
            .whenComplete((_, throwable) -> {
              if (throwable != null) {
                logger.warn("Failed to delete message {}/{}/{} from FoundationDb", accountIdentifier.uuid(), deviceId,
                    messageGuid, throwable);
              }
            });
      }
    }

    return redisDynamoDbMessageStream.acknowledgeMessage(messageGuid, serverTimestamp);
  }

  private synchronized void verifyMessageAgreement(final MessageStreamEntry.Envelope envelope,
      final Stream stream) {

    if (stopAgreementVerification) {
      return;
    }

    final UUID messageUUID = UUIDUtil.fromByteString(envelope.message().getServerGuid());

    if (envelope.message().getEphemeral()) {
      // We don't compare ephemeral messages across streams because the two systems have different ways of handling and
      // delivering ephemeral messages. However, we track recently seen ephemeral UUIDs on the Redis/Dynamo stream
      // to answer whether a message missing from FoundationDB was ephemeral or not.
      if (stream == Stream.REDIS_DYNAMO) {
        recentRedisDynamoEphemeralUuids.add(messageUUID);
      }
      return;
    }

    // There could be a race where a FoundationDB message is delivered on the stream right after it was acknowledged,
    // so we don't want to add it to the window again; check it against a set of recently seen UUIDs before adding.
    if (Stream.FOUNDATION_DB == stream && !recentFoundationDbUuids.add(messageUUID)) {
      return;
    }

    final Map<UUID, HashCode> thisStreamWindow;
    final Map<UUID, HashCode> otherStreamWindow;
    if (Stream.REDIS_DYNAMO == stream) {
      thisStreamWindow = redisDynamoMessageWindow;
      otherStreamWindow = foundationDbMessageWindow;
    } else {
      thisStreamWindow = foundationDbMessageWindow;
      otherStreamWindow = redisDynamoMessageWindow;
    }

    final HashCode thisStreamMessageHashCode = hashCode(envelope.message());

    final HashCode otherStreamMessageHashCode = otherStreamWindow.remove(messageUUID);
    if (otherStreamMessageHashCode != null) {
      if (!otherStreamMessageHashCode.equals(thisStreamMessageHashCode)) {
        MESSAGE_MISMATCHES_COUNTER.increment();
      }
    } else {
      thisStreamWindow.put(messageUUID, thisStreamMessageHashCode);
      if (thisStreamWindow.size() >= MAX_WINDOW_SIZE) {
        // Stream is way ahead of the other, stop comparing further
        stopAgreementVerification = true;
        thisStreamWindow.clear();
        otherStreamWindow.clear();
        recentFoundationDbUuids.clear();
        setAgreementFailure(Tags.of("reason", "overflow", "stream", stream.name));
      }
    }
  }

  private synchronized boolean messageNotDeliveredOnFoundationDbStream(final UUID messageGuid) {
    return redisDynamoMessageWindow.containsKey(messageGuid);
  }

  private synchronized void setAgreementFailure(final Tags tags) {
    if (agreementFailureTags == null) {
      agreementFailureTags = tags;
    }
  }

  private synchronized void publishStreamAgreementMetrics() {

    Tags tags = Tags.of("success", String.valueOf(agreementFailureTags == null));
    if (agreementFailureTags != null) {
      tags = tags.and(agreementFailureTags);
    }

    Metrics.counter(STREAM_AGREEMENTS, tags).increment();
  }

  private synchronized void handleMissingFoundationDbMessage(final UUID messageGuid) {
    final Boolean ephemeral;
    if (redisDynamoMessageWindow.containsKey(messageGuid)) {
      // The comparison window always contains non-ephemeral messages since we ignore ephemeral messages for comparison
      ephemeral = false;
    } else if (recentRedisDynamoEphemeralUuids.contains(messageGuid)) {
      ephemeral = true;
    } else {
      ephemeral = null;
    }
    setAgreementFailure(Tags.of("reason", "missingMessages"));
    Metrics.counter(MISSING_MESSAGES_COUNTER, "ephemeral", ephemeral == null ? "unknown" : ephemeral.toString())
        .increment();
  }

  private static HashCode hashCode(final MessageProtos.Envelope envelope) {
    final byte[] serialized = new byte[envelope.getSerializedSize()];
    final CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(serialized);
    codedOutputStream.useDeterministicSerialization();
    try {
      envelope.writeTo(codedOutputStream);
      codedOutputStream.flush();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return Hashing.murmur3_128().hashBytes(serialized);
  }

}
