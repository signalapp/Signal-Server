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

  // Indicates whether we've sent an "initial queue drain complete" signal to subscribers. This state will transition
  // from "not ready" to "pending" as soon as the initial message source subscription completes, and then from "pending"
  // to "send" when the signal has been sent. This state will never roll backwards.
  private QueueEmptySignalState queueEmptySignalState = QueueEmptySignalState.NOT_READY;

  // The total requested demand from the subscriber across the whole lifetime of this publisher
  private long requestedDemand = 0;

  // The total number of signals (new messages or "queue empty" signals) published during the lifetime of this
  // publisher. Must not exceed `requestedDemand`.
  private long publishedEntries = 0;

  // The total number of published signals that have been acknowledged by the subscriber; to avoid re-reading messages,
  // we should never start a new message source subscriber until `acknowledgedEntries` is equal to `publishedEntries`
  private long acknowledgedEntries = 0;

  // Although technically nullable, operation of this publisher really begins once we get a subscriber. This publisher
  // supports only a single subscriber.
  @Nullable private Flow.Subscriber<? super MessageStreamEntry> subscriber;

  // If terminated (i.e. by an error or by downstream cancellation), this publisher will stop emitting signals. Once
  // terminated, a publisher cannot be un-terminated.
  private boolean terminated = false;

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

  private enum QueueEmptySignalState {
    // Indicates that we are not yet ready to send the "initial queue drain complete" signal regardless of outstanding
    // demand
    NOT_READY,

    // Indicates that we are ready to send the "queue empty" signal as soon as demand is available
    PENDING,

    // Indicates that we have sent the "queue empty" signal and must never send it again
    SENT
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
      redisDynamoDbMessagePublisher.handleMessageSourceSubscribed(subscription);
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
      terminate();
      subscriber.onError(new ConflictingMessageConsumerException());
    }
  }

  synchronized void handleMessageAcknowledged() {
    acknowledgedEntries += 1;
    assert acknowledgedEntries <= publishedEntries;

    maybeGenerateMessageSource();
  }

  private synchronized boolean maybeSendQueueEmptySignal() {
    // Regardless of any other state, don't do anything if terminated
    if (terminated) {
      return false;
    }

    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    if (queueEmptySignalState == QueueEmptySignalState.PENDING && publishedEntries < requestedDemand) {
      queueEmptySignalState = QueueEmptySignalState.SENT;
      publishedEntries += 1;

      // Subscribers don't explicitly acknowledge "queue empty" signals, and we can consider them automatically
      // acknowledged
      acknowledgedEntries += 1;

      subscriber.onNext(new MessageStreamEntry.QueueEmpty());

      assert publishedEntries <= requestedDemand;
      assert acknowledgedEntries <= publishedEntries;

      return true;
    }

    return false;
  }

  private synchronized void maybeGenerateMessageSource() {
    if (terminated) {
      // Regardless of any other state, don't do anything if terminated
      return;
    }

    if (messageSourceSubscriber != null) {
      // We're still working through a previous message source; wait until it completes before starting a new one.
      return;
    }

    if (storedMessageState == StoredMessageState.EMPTY) {
      // We don't think there are any messages in either message source; don't do anything until the situation changes
      // (when new messages arrive, we'll come back to this point with a non-empty stored message state)
      return;
    }

    if (publishedEntries == requestedDemand) {
      // Even if there are messages available, there's no demand for them yet (when there's new demand, we'll come back
      // to this point with a higher value for `requestedDemand` via `addDemand`)
      return;
    }

    if (acknowledgedEntries < publishedEntries) {
      // To avoid double-reading messages from data stores that don't support cursors, don't get a new message source
      // unless all previously-published signals have been acknowledged (when messages are acknowledged, we'll come back
      // to this point with a higher value for `acknowledgedEntries` via `handleMessageAcknowledged`)
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

  private synchronized void handleMessageSourceSubscribed(final Subscription subscription) {
    if (!terminated) {
      // If we already have some unmet demand, pass that on to the upstream publisher immediately on subscribing
      if (requestedDemand > publishedEntries) {
        subscription.request(requestedDemand - publishedEntries);
      }
    }
  }

  private synchronized void handleNextMessage(final MessageProtos.Envelope message) {
    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    if (!terminated) {
      // We only pass along unfulfilled demand to the message source subscriber, so if the message source subscriber
      // emits a new signal, it should fit within the unfulfilled demand from the downstream subscriber
      assert publishedEntries < requestedDemand;

      publishedEntries += 1;
      subscriber.onNext(new MessageStreamEntry.Envelope(message));
    }
  }

  private synchronized void handleMessageSourceComplete() {
    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    messageSourceSubscriber = null;

    if (queueEmptySignalState == QueueEmptySignalState.NOT_READY) {
      queueEmptySignalState = QueueEmptySignalState.PENDING;
      maybeSendQueueEmptySignal();
    }

    // New messages may have arrived already; fetch them if possible
    maybeGenerateMessageSource();
  }

  private synchronized void handleMessageSourceError(final Throwable throwable) {
    // The machinery that produces messages won't activate until we have a subscriber
    assert subscriber != null;

    if (!terminated) {
      terminate();
      subscriber.onError(throwable);
    }
  }

  private synchronized void addDemand(final long demand) {
    if (demand <= 0) {
      throw new IllegalArgumentException("Demand must be positive");
    }

    requestedDemand += demand;

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

  private synchronized void terminate() {
    if (!terminated) {
      terminated = true;

      if (messageSourceSubscriber != null) {
        messageSourceSubscriber.dispose();
        messageSourceSubscriber = null;
      }

      // Stop receiving signals about new messages/conflicting consumers
      redisMessageAvailabilityManager.handleClientDisconnected(accountIdentifier, device.getId());
    }
  }
}
