/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import java.util.UUID;
import java.util.concurrent.Flow;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.MessageAvailabilityListener;
import org.whispersystems.textsecuregcm.push.RedisMessageAvailabilityManager;
import reactor.core.observability.micrometer.Micrometer;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/// A Redis/DynamoDB message publisher produces a non-terminating stream of messages for a specific device. It listens
/// for message availability signals from [RedisMessageAvailabilityManager] and emits new messages to its subscriber
/// when available.
///
/// This publisher supports only a single subscriber. It assumes that subscribers acknowledge (delete) messages as they
/// read the messages, and may emit duplicate messages if subscribers do not acknowledge messages before requesting more
/// messages.
class RedisDynamoDbMessagePublisher implements MessageAvailabilityListener, Flow.Publisher<MessageStreamEntry> {

  private final MessagesDynamoDb messagesDynamoDb;
  private final MessagesCache messagesCache;
  private final RedisMessageAvailabilityManager redisMessageAvailabilityManager;

  private final UUID accountIdentifier;
  private final Device device;

  // Indicates which data source(s) we think might contain messages for the destination device. Messages initially land
  // in Redis, but are eventually "persisted" to DynamoDB. This state changes in response to signals this publisher
  // receives as a MessageAvailabilityListener. As an initial state, we assume that we have messages in both DynamoDB
  // and Redis.
  private StoredMessageState storedMessageState = StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE;

  // The number of messages the downstream subscriber is ready to receive. This changes in response to new requests from
  // the downstream subscriber and gets decremented every time we publish a message.
  private long unmetDemand = 0;

  // Although technically nullable, operation of this publisher really begins once we get a subscriber. This publisher
  // supports only a single subscriber.
  @Nullable private Flow.Subscriber<? super MessageStreamEntry> subscriber;

  // If terminated (i.e. by an error or by downstream cancellation), this publisher will stop emitting signals. Once
  // terminated, a publisher cannot be un-terminated.
  private boolean terminated = false;

  // This publisher will emit exactly one "queue empty" signal once the initial contents of the message queue have been
  // drained. Once emitted, this flag is set to `true` and will never change again.
  private boolean publishedQueueEmptySignal = false;

  // …but we may not be able to send the "queue empty" signal downstream immediately if there's no demand. This flag
  // tracks whether we're ready to publish a "queue empty" signal, regardless of whether we've actually sent it. Once
  // this flag is set to `true`, it will never change again.
  private boolean readyToPublishQueueEmptySignal = false;

  // A message source subscriber subscribes to messages from upstream data sources (i.e. DynamoDB and Redis), and this
  // publisher relays signals the message source subscriber to the downstream subscriber. The message source subscriber
  // may be null if we're not actively fetching messages from an upstream source and changes every time an upstream
  // publisher completes.
  @Nullable private MessageSourceSubscriber messageSourceSubscriber;

  private static final String GET_MESSAGES_FOR_DEVICE_FLUX_NAME =
      name(RedisDynamoDbMessagePublisher.class, "getMessagesForDevice");

  private enum StoredMessageState {
    // Indicates that stored messages are available in at least DynamoDB and possibly also Redis
    PERSISTED_NEW_MESSAGES_AVAILABLE,

    // Indicates that messages are available in Redis, but have not yet been persisted to DynamoDB
    CACHED_NEW_MESSAGES_AVAILABLE,

    // Indicates that no new messages are available in either Redis or DynamoDB
    EMPTY
  }

  /// A message source subscriber subscribes to upstream message source publishers and relays signals to the downstream
  /// subscriber via the parent `RedisDynamoDbMessagePublisher`.
  private static class MessageSourceSubscriber extends BaseSubscriber<MessageProtos.Envelope> {

    private final RedisDynamoDbMessagePublisher redisDynamoDbMessagePublisher;

    private MessageSourceSubscriber(RedisDynamoDbMessagePublisher redisDynamoDbMessagePublisher) {
        this.redisDynamoDbMessagePublisher = redisDynamoDbMessagePublisher;
    }

    @Override
    protected void hookOnSubscribe(final Subscription subscription) {
      final long unmetDemand = redisDynamoDbMessagePublisher.getUnmetDemand();

      // If we already have some unmet demand, pass that on to the upstream publisher immediately on subscribing
      if (unmetDemand > 0) {
        subscription.request(unmetDemand);
      }
    }

    @Override
    protected void hookOnNext(final MessageProtos.Envelope message) {
      redisDynamoDbMessagePublisher.handleNextMessage(message);
    }

    @Override
    protected void hookOnComplete() {
      redisDynamoDbMessagePublisher.handleMessageSourceComplete();
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      redisDynamoDbMessagePublisher.handleMessageSourceError(throwable);
    }
  }

  RedisDynamoDbMessagePublisher(final MessagesDynamoDb messagesDynamoDb,
      final MessagesCache messagesCache,
      final RedisMessageAvailabilityManager redisMessageAvailabilityManager,
      final UUID accountIdentifier,
      final Device device) {

    this.messagesDynamoDb = messagesDynamoDb;
    this.messagesCache = messagesCache;
    this.redisMessageAvailabilityManager = redisMessageAvailabilityManager;
    this.accountIdentifier = accountIdentifier;
    this.device = device;
  }

  @Override
  public synchronized void subscribe(final Flow.Subscriber<? super MessageStreamEntry> subscriber) {
    if (this.subscriber != null) {
      subscriber.onError(new IllegalStateException("Redis/DynamoDB message publisher only allows one subscriber"));
      return;
    }

    this.subscriber = subscriber;

    // Listen for signals indicating that new messages are available in Redis, that messages have been persisted from
    // Redis to DynamoDB, or that there's a conflicting message reader connected somewhere else
    redisMessageAvailabilityManager.handleClientConnected(accountIdentifier, device.getId(), this);

    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(final long n) {
        addDemand(n);
      }

      @Override
      public void cancel() {
        terminate();
      }
    });
  }

  @Override
  public synchronized void handleNewMessageAvailable() {
    // We only need to take action if we think there aren't already messages to pass downstream. Any other stored
    // message state implies that we're either actively sending messages downstream or we're waiting for demand from the
    // downstream subscriber and don't need to take any action now. We'll call `maybeGenerateMessageSource` either when
    // we receive a request for more messages or when the current upstream publisher completes.
    if (storedMessageState == StoredMessageState.EMPTY) {
      storedMessageState = StoredMessageState.CACHED_NEW_MESSAGES_AVAILABLE;
      maybeGenerateMessageSource();
    }
  }

  @Override
  public synchronized void handleMessagesPersisted() {
    // We only need to take action if we think there aren't already messages in DynamoDB. If we're already aware of
    // messages in DynamoDB, then we're either actively sending messages downstream or we're waiting for demand from the
    // downstream subscriber and don't need to take any action now. We'll call `maybeGenerateMessageSource` either when
    // we receive a request for more messages or when the current upstream publisher completes.
    if (storedMessageState != StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE) {
      storedMessageState = StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE;
      maybeGenerateMessageSource();
    }
  }

  @Override
  public synchronized void handleConflictingMessageConsumer() {
    // We don't register as a listener for conflicting consumer signals until we have a subscriber
    assert subscriber != null;

    if (!terminated) {
      subscriber.onError(new ConflictingMessageConsumerException());
    }

    terminate();
  }

  private synchronized boolean maybeSendQueueEmptySignal() {
    // Regardless of any other state, don't do anything if terminated
    if (terminated) {
      return false;
    }

    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    if (readyToPublishQueueEmptySignal && !publishedQueueEmptySignal && getUnmetDemand() > 0) {
      subscriber.onNext(new MessageStreamEntry.QueueEmpty());
      unmetDemand -= 1;

      assert unmetDemand >= 0;

      publishedQueueEmptySignal = true;

      return true;
    }

    return false;
  }

  private synchronized void maybeGenerateMessageSource() {
    // Regardless of any other state, don't do anything if terminated
    if (terminated) {
      return;
    }

    if (storedMessageState == StoredMessageState.EMPTY || unmetDemand == 0) {
      // We don't think there are any messages in either source or there's no demand for messages; either way, wait for
      // things to change before trying to generate a message source
      return;
    }

    // We maybe be able to skip reading from DynamoDB entirely if we think messages are only stored in Redis
    final Publisher<MessageProtos.Envelope> dynamoPublisher =
        storedMessageState == StoredMessageState.PERSISTED_NEW_MESSAGES_AVAILABLE
            ? messagesDynamoDb.load(accountIdentifier, device, null)
            : Flux.empty();

    final Publisher<MessageProtos.Envelope> redisPublisher = messagesCache.get(accountIdentifier, device.getId());

    final Flux<MessageProtos.Envelope> messageSource = Flux.concat(dynamoPublisher, redisPublisher)
        .name(GET_MESSAGES_FOR_DEVICE_FLUX_NAME)
        .tap(Micrometer.metrics(Metrics.globalRegistry));

    messageSourceSubscriber = new MessageSourceSubscriber(this);
    messageSource.subscribe(messageSourceSubscriber);

    // If nothing else happens before the DynamoDB/Redis publisher completes, then we'll have emptied all stored
    // messages; new signals about persisted messages or newly-arrived messages will change this state
    storedMessageState = StoredMessageState.EMPTY;
  }

  private synchronized void handleNextMessage(final MessageProtos.Envelope message) {
    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    if (!terminated) {
      unmetDemand -= 1;
      assert unmetDemand >= 0;

      subscriber.onNext(new MessageStreamEntry.Envelope(message));
    }
  }

  private synchronized void handleMessageSourceComplete() {
    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    messageSourceSubscriber = null;

    // Attempt to send a "queue empty" signal if we haven't already
    readyToPublishQueueEmptySignal = true;
    maybeSendQueueEmptySignal();

    // New messages may have arrived already; fetch them if possible
    maybeGenerateMessageSource();
  }

  private synchronized void handleMessageSourceError(final Throwable throwable) {
    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    if (!terminated) {
      subscriber.onError(throwable);
      terminate();
    }
  }

  private synchronized void addDemand(final long demand) {
    if (demand <= 0) {
      throw new IllegalArgumentException("Demand must be positive");
    }

    unmetDemand += demand;

    // We may have been waiting for non-zero demand before sending a "queue empty" signal
    final boolean sentQueueEmptySignal = maybeSendQueueEmptySignal();

    // This is a little tricky; if we already have a subscriber, we only want to request NEW demand, not the total
    // outstanding demand. On top of that, we may have consumed some demand by sending a "queue empty" message.
    final long newDemand = demand - (sentQueueEmptySignal ? 1 : 0);

    if (newDemand > 0) {
      if (messageSourceSubscriber != null) {
        messageSourceSubscriber.request(newDemand);
      } else {
        maybeGenerateMessageSource();
      }
    }
  }

  private synchronized long getUnmetDemand() {
    return unmetDemand;
  }

  private synchronized void terminate() {
    if (!terminated) {
      terminated = true;

      // Stop receiving signals about new messages/conflicting consumers
      redisMessageAvailabilityManager.handleClientDisconnected(accountIdentifier, device.getId());

      if (messageSourceSubscriber != null) {
        messageSourceSubscriber.dispose();
      }
    }
  }
}
