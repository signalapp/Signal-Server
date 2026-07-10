package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/// Publishes a message stream from a device queue in FoundationDB. Capable of publishing both a finite stream for
/// catching up to end-of-queue,and an infinite stream for live updates.
class FoundationDbMessagePublisher {

  private final Database database;
  private final Clock clock;
  /// Keeps track of the key from which to start reading on the next iteration
  private volatile KeySelector beginKeyCursor;
  /// The end key at which we stop reading messages. For finite publisher, this is just past the end-of-queue key at the
  /// time the publisher was created. For an infinite publisher, this is the end of subspace range.
  private final KeySelector endKeyExclusive;

  /// The key that stores the server ID/timestamp indicating when/where the connected client was last known to be
  /// present. Active publishers refresh this key at regular intervals (see [#renewPresenceFuture]).
  @Nullable private final byte[] presenceKey;

  // An ID for this specific stream for use when checking "ownership" of a presence value.
  private final int streamId = NEXT_STREAM_ID.getAndIncrement();

  /// The key that is updated whenever new messages are available in the queue. If null, it is inferred that the publisher
  /// is finite and will terminate when the end-of-queue at time of publisher creation is reached. Otherwise, the publisher
  /// is "infinite" and will continue to wait for new messages and publish them in a loop.
  @Nullable private final byte[] messagesAvailableWatchKey;

  /// Listener to watch for state machine transitions; used for testing.
  private final BiConsumer<State, State> stateChangeListener;

  /// Whether the publisher is finite or infinite.
  private final boolean terminateOnQueueEmpty;

  /// Tracks the current state of the publisher state machine. Initial state presumes that messages are available in the queue.
  private State state = State.MESSAGES_AVAILABLE;

  private final Flux<FoundationDbMessageStreamEntry.Message> messagePublisher;

  /// Reference to the sink we publish messages to.
  private volatile FluxSink<FoundationDbMessageStreamEntry.Message> emitter;

  /// Future that completes when the watch for {@link #messagesAvailableWatchKey} triggers.
  private CompletableFuture<Void> watchFuture;

  /// An executor service for refreshing the connected client's presence value at regular intervals.
  @Nullable final ScheduledExecutorService presenceRenewalExecutorService;

  /// A future that represents the next pending presence renewal.
  @Nullable private CompletableFuture<?> renewPresenceFuture;

  private long totalRequested;
  private long totalEmitted;

  private static final AtomicInteger NEXT_STREAM_ID = new AtomicInteger();

  private static final long RENEWAL_INTERVAL_MILLIS =
      FoundationDbMessageStore.PRESENCE_STALE_THRESHOLD.multipliedBy(4).dividedBy(5).toMillis();

  private static final int MAX_MESSAGES_PER_PAGE = 1024;

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDbMessagePublisher.class);

  private static final String WATCHES_COUNTER = MetricsUtil.name(FoundationDbMessagePublisher.class, "watches");
  private static final String ACTION_TAG = "action";

  enum State {
    /// Messages are likely available in the queue. Initial state.
    MESSAGES_AVAILABLE,
    /// We're actively fetching messages from FoundationDB.
    FETCHING_MESSAGES,
    /// We have read all available messages in the queue.
    QUEUE_EMPTY,
    /// The message available watch triggered while other stuff was going on. We record it as a state so that we can
    /// fetch new messages at a more opportune time.
    MESSAGE_AVAILABLE_SIGNAL_BUFFERED,
    /// Waiting for the message available watch to trigger that signals new messages have arrived.
    AWAITING_NEW_MESSAGES,
    /// Terminal state indicating no more messages will be published. Either we have successfully published all messages
    /// in the case of a finite publisher, or the subscriber has indicated that it has terminated.
    TERMINATED,
    /// Terminal state reached when there is an error fetching or publishing messages.
    ERROR
  }

  enum Event {
    /// Downstream subscriber requested more items.
    DEMAND_REQUESTED,
    /// The FoundationDB range query returned less than the batch size.
    FETCHED_ALL_AVAILABLE_MESSAGES,
    /// We have successfully fetched and published a message batch
    PUBLISHED_MESSAGES,
    /// The message available watch triggered indicating new messages have arrived.
    MESSAGE_AVAILABLE_WATCH_TRIGGERED,
    /// Internal self-trigger used to immediately transition to the next state.
    INTERNAL_TRIGGER,
    /// An error occurred during fetching from FoundationDB or publishing messages to the sink.
    FETCH_OR_PUBLISH_ERROR_OCCURRED
  }

  FoundationDbMessagePublisher(
      final Database database,
      final Clock clock,
      final KeySelector beginKeyInclusive,
      final KeySelector endKeyExclusive,
      @Nullable final byte[] presenceKey,
      @Nullable final ScheduledExecutorService presenceRenewalExecutorService,
      @Nullable final byte[] messagesAvailableWatchKey,
      @Nullable final BiConsumer<State, State> stateChangeListener) {

    this.database = database;
    this.clock = clock;
    this.beginKeyCursor = beginKeyInclusive;
    this.endKeyExclusive = endKeyExclusive;
    this.presenceKey = presenceKey;
    this.presenceRenewalExecutorService = presenceRenewalExecutorService;
    this.messagesAvailableWatchKey = messagesAvailableWatchKey;
    this.terminateOnQueueEmpty = messagesAvailableWatchKey == null;
    this.stateChangeListener = stateChangeListener != null ? stateChangeListener : (_, _) -> {};

    if (!terminateOnQueueEmpty) {
      Objects.requireNonNull(presenceKey);
      Objects.requireNonNull(presenceRenewalExecutorService);
    }

    final Mono<Void> establishPresence = terminateOnQueueEmpty
        ? Mono.empty()
        // Establish presence before attempting to fetch messages. We must establish presence for watches to work as
        // expected. If we fail to establish presence, then this non-terminating stream will never learn about new
        // messages. If establishing presence throws an exception, then the whole publisher will terminate with that
        // exception.
        : Mono.fromFuture(this::renewPresence).then(Mono.fromRunnable(this::schedulePresenceRenewal));

    this.messagePublisher = establishPresence
        .thenMany(Flux.create(emitter -> {
          this.emitter = emitter;
          emitter.onRequest(this::addDemand);
          emitter.onDispose(this::onDispose);
        }));
  }

  /// Creates a [FoundationDbMessagePublisher] that publishes a stream of messages in a queue that terminates when it
  /// yields the last message that was present in the queue at the time the publisher was created. This is helpful in
  /// cases when callers need to "catch up" on stored messages without following fresh updates (for example, when a
  /// client first connects and needs to load stored messages before receiving a "live" stream of new messages).
  public static FoundationDbMessagePublisher createFinitePublisher(
      final Database database,
      final Clock clock,
      final KeySelector beginKeyInclusive,
      final KeySelector endKeyExclusive) {

    return new FoundationDbMessagePublisher(database,
        clock,
        beginKeyInclusive,
        endKeyExclusive,
        null,
        null,
        null,
        null);
  }

  /// Creates a [FoundationDbMessagePublisher] that publishes a non-terminating stream of messages from a device queue.
  /// It waits for new messages and publishes them in a loop. Useful when a client has finished receiving its stored
  /// messages and is now waiting for a live stream of new messages.
  public static FoundationDbMessagePublisher createInfinitePublisher(
      final Database database,
      final Clock clock,
      final KeySelector beginKeyInclusive,
      final KeySelector endKeyExclusive,
      final byte[] presenceKey,
      final ScheduledExecutorService presenceRenewalExecutorService,
      final byte[] messagesAvailableWatchKey) {

    return new FoundationDbMessagePublisher(database,
        clock,
        beginKeyInclusive,
        endKeyExclusive,
        presenceKey,
        presenceRenewalExecutorService,
        messagesAvailableWatchKey,
        null);
  }

  private synchronized void setState(final State newState, final Event event) {
    LOGGER.debug("Transitioned from state {} to state {} on event {}", state, newState, event);
    final State oldState = state;
    state = newState;
    stateChangeListener.accept(oldState, newState);
  }

  /// Called when an event occurs. Transitions the current state to a new state if this is a defined transition.
  ///
  /// @param event the {@link Event} that has occurred
  protected synchronized void transitionStateOnEvent(final Event event) {
    assert emitter != null;
    boolean knownTransition = true;
    switch (state) {
      case MESSAGES_AVAILABLE -> {
        switch (event) {
          case DEMAND_REQUESTED -> {
            setState(State.FETCHING_MESSAGES, event);
            emitMessages();
          }
          case INTERNAL_TRIGGER -> {
            if (emitter.requestedFromDownstream() > 0) {
              setState(State.FETCHING_MESSAGES, event);
              emitMessages();
            }
          }
          default -> knownTransition = false;
        }
      }
      case FETCHING_MESSAGES -> {
        switch (event) {
          case FETCHED_ALL_AVAILABLE_MESSAGES -> setState(State.QUEUE_EMPTY, event);
          case PUBLISHED_MESSAGES -> {
            setState(State.MESSAGES_AVAILABLE, event);
            transitionStateOnEvent(Event.INTERNAL_TRIGGER);
          }
          case MESSAGE_AVAILABLE_WATCH_TRIGGERED -> setState(State.MESSAGE_AVAILABLE_SIGNAL_BUFFERED, event);
          case FETCH_OR_PUBLISH_ERROR_OCCURRED -> setState(State.ERROR, event);
          default -> knownTransition = false;
        }
      }
      case QUEUE_EMPTY -> {
        if (terminateOnQueueEmpty) {
          switch (event) {
            case PUBLISHED_MESSAGES -> {
              setState(State.TERMINATED, event);
              emitter.complete();
            }
            case FETCHED_ALL_AVAILABLE_MESSAGES -> setState(State.ERROR, event);
            default -> knownTransition = false;
          }
        } else {
          switch (event) {
            case MESSAGE_AVAILABLE_WATCH_TRIGGERED -> setState(State.MESSAGE_AVAILABLE_SIGNAL_BUFFERED, event);
            case PUBLISHED_MESSAGES -> setState(State.AWAITING_NEW_MESSAGES, event);
            default -> knownTransition = false;
          }
        }
      }
      case MESSAGE_AVAILABLE_SIGNAL_BUFFERED -> {
        switch (event) {
          case PUBLISHED_MESSAGES -> {
            setState(State.MESSAGES_AVAILABLE, event);
            transitionStateOnEvent(Event.INTERNAL_TRIGGER);
          }
          default -> knownTransition = false;
        }
      }
      case AWAITING_NEW_MESSAGES -> {
        switch (event) {
          case MESSAGE_AVAILABLE_WATCH_TRIGGERED -> {
            setState(State.MESSAGES_AVAILABLE, event);
            transitionStateOnEvent(Event.INTERNAL_TRIGGER);
          }
          default -> knownTransition = false;
        }
      }
      default -> knownTransition = false;
    }
    if (!knownTransition) {
      LOGGER.debug("Unhandled event {} on state {}", event, state);
    }
  }

  /// Fetch messages using a range query limiting batch size to `maxMessages`. If the query returns fewer than
  /// `maxMessages`, emit [Event#FETCHED_ALL_AVAILABLE_MESSAGES]. In the case of an infinite publisher, also set
  /// a watch for new messages. Additionally, the cursor is updated so that we begin fetching from the right key on
  /// subsequent scans.
  ///
  /// @param maxMessages the maximum number of messages to retrieve
  ///
  /// @return a future of a list of [FoundationDbMessageStreamEntry.Message] with a max size of `maxMessages`
  private CompletableFuture<List<FoundationDbMessageStreamEntry.Message>> getMessagesBatch(final int maxMessages) {
    if (maxMessages <= 0) {
      throw new IllegalArgumentException("Max messages must be positive");
    }

    return FoundationDbUtil.safeRunAsync(database, transaction ->
            transaction.getRange(beginKeyCursor, endKeyExclusive, maxMessages, false, StreamingMode.EXACT).asList()
                .thenApply(keyValues -> {
                  if (keyValues.size() < maxMessages && !terminateOnQueueEmpty) {
                    setWatch(transaction);
                  }

                  return keyValues;
                }))
        .thenApply(keyValues -> {
          if (keyValues.size() < maxMessages) {
            transitionStateOnEvent(Event.FETCHED_ALL_AVAILABLE_MESSAGES);
          }

          if (keyValues.isEmpty()) {
            return Collections.emptyList();
          }

          beginKeyCursor = KeySelector.firstGreaterThan(keyValues.getLast().getKey());

          return keyValues.stream()
              .map(keyValue -> {
                final Versionstamp versionstamp = FoundationDbMessageStore.getVersionstamp(keyValue.getKey());

                try {
                  return new FoundationDbMessageStreamEntry.Message(versionstamp,
                      MessageProtos.Envelope.parseFrom(keyValue.getValue()));
                } catch (final InvalidProtocolBufferException e) {
                  throw new UncheckedIOException(e);
                }
              })
              .toList();
        });
  }

  private synchronized void addDemand(final long n) {
    totalRequested += n;

    assert totalRequested > totalEmitted;

    transitionStateOnEvent(Event.DEMAND_REQUESTED);
  }

  private synchronized void handleEntriesEmitted(final long n) {
    totalEmitted += n;
  }

  /// Returns the unmet demand requested by subscribers. Values larger than [Integer#MAX_VALUE] are clamped to
  /// [Integer#MAX_VALUE].
  ///
  /// @return the unmet demand requested by publishers as an `int`
  private synchronized int getOutstandingDemand() {
    return (int) Math.min(totalRequested - totalEmitted, Integer.MAX_VALUE);
  }

  /// Fetch and publish messages. Messages are fetched in batches to avoid excessive memory consumption. After each
  /// fetch operation, [#beginKeyCursor] is updated to the next key we need to start reading from. If the fetch
  /// operation returns fewer items than the batch size, we infer that we have fetched all available messages and
  /// [Event#FETCHED_ALL_AVAILABLE_MESSAGES] is sent to the state machine. See [#getMessagesBatch(int)] for details.
  /// Additionally, after we successfully publish the batch of messages, {@link Event#PUBLISHED_MESSAGES} is emitted. If
  /// there's an error while fetching or publishing, [Event#FETCH_OR_PUBLISH_ERROR_OCCURRED] is emitted instead.
  private void emitMessages() {
    final int maxMessages = Math.min(getOutstandingDemand(), MAX_MESSAGES_PER_PAGE);

    getMessagesBatch(maxMessages)
        .thenAccept(messageStreamEntries -> {
          messageStreamEntries.forEach(emitter::next);
          handleEntriesEmitted(messageStreamEntries.size());
          transitionStateOnEvent(Event.PUBLISHED_MESSAGES);
        })
        .exceptionally(t -> {
          transitionStateOnEvent(Event.FETCH_OR_PUBLISH_ERROR_OCCURRED);
          emitter.error(t);
          return null;
        });
  }

  /// Get the stream of messages.
  ///
  /// @return [Flux] of messages
  public Flux<FoundationDbMessageStreamEntry.Message> getMessages() {
    return this.messagePublisher;
  }

  private synchronized void setWatch(final Transaction transaction) {
    assert messagesAvailableWatchKey != null;
    if (state == State.TERMINATED || state == State.ERROR) {
      // If we're already in a terminal state, we don't want to set a watch that would leak
      return;
    }
    // Set a watch that will be triggered when new messages arrive. When it is triggered, we attempt to fetch messages
    // again (if there is demand). When we run out of messages, this method will be called again, setting another watch,
    // and so on, thus achieving a "watch for new messages -> read -> publish" loop.
    watchFuture = transaction.watch(messagesAvailableWatchKey);
    Metrics.counter(WATCHES_COUNTER, ACTION_TAG, "set").increment();
    watchFuture.thenRun(() -> {
      Metrics.counter(WATCHES_COUNTER, ACTION_TAG, "triggered").increment();
      transitionStateOnEvent(Event.MESSAGE_AVAILABLE_WATCH_TRIGGERED);
    });
  }

  /// Cancel the watch (if any). Although inactive watches are automatically timed-out, we explicitly cancel when the
  /// subscription is disposed to clean up associated resources and to avoid having too many watches open at once.
  private synchronized void cancelWatch() {
    if (watchFuture != null) {
      if (watchFuture.cancel(true)) {
        Metrics.counter(WATCHES_COUNTER, ACTION_TAG, "cancelled").increment();
      }
      watchFuture = null;
    }
  }

  private synchronized void schedulePresenceRenewal() {
    if (state == State.TERMINATED || state == State.ERROR) {
      return;
    }

    assert presenceRenewalExecutorService != null;

    renewPresenceFuture = new CompletableFuture<>();

    final ScheduledFuture<?> scheduledFuture = presenceRenewalExecutorService.schedule(() -> {
      renewPresence().whenComplete((_, throwable) -> {
        if (throwable != null) {
          LOGGER.warn("Failed to renew presence", throwable);
          renewPresenceFuture.completeExceptionally(throwable);
        } else {
          renewPresenceFuture.complete(null);
        }
      });
    }, RENEWAL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);

    renewPresenceFuture.whenComplete((_, throwable) -> {
      if (throwable != null) {
        // We can wind up here in one of two ways:
        //
        // 1. We're shutting down the publisher, in which case `throwable` is a `CancellationException`. We don't NEED
        //    to terminate the publisher in this case (since it's already terminated), but it's harmless to issue a
        //    second termination request, which will just be a no-op.
        // 2. Something unexpected has gone wrong. We DO need to terminate the publisher in this case, because a failure
        //    to renew presence means that we won't get notified of new messages and so the publisher is in a bad state.
        //
        // In both cases, we should stop trying to schedule presence renewal.
        scheduledFuture.cancel(false);
        terminateWithError(throwable);
      } else {
        // We renewed presence successfully and the publisher is still active; renew presence again at some point in the
        // future.
        schedulePresenceRenewal();
      }
    });
  }

  private synchronized CompletableFuture<Void> renewPresence() {
    if (state == State.TERMINATED || state == State.ERROR) {
      return CompletableFuture.failedFuture(new IllegalStateException("Publisher already terminated"));
    }

    return FoundationDbUtil.safeRunAsync(database, transaction -> {
      transaction.set(presenceKey, FoundationDbMessageStore.getPresenceValue(clock.instant(), streamId));
      return CompletableFuture.completedFuture(null);
    });
  }

  @VisibleForTesting
  synchronized void clearPresence() {
    if (renewPresenceFuture != null) {
      renewPresenceFuture.cancel(true);

      renewPresenceFuture.whenComplete((_, _) ->
          FoundationDbUtil.safeRunAsync(database, transaction -> transaction.get(presenceKey).thenAccept(presenceValue -> {
                if (presenceValue == null) {
                  return;
                }

                final byte[] presenceServerId = FoundationDbMessageStore.getPresenceServerId(presenceValue);
                final int presenceStreamId = FoundationDbMessageStore.getPresenceStreamId(presenceValue);

                if (Arrays.equals(presenceServerId, FoundationDbMessageStore.getServerId()) && streamId == presenceStreamId) {
                  transaction.clear(presenceKey);
                }
              }))
              .whenComplete((_, throwable) -> {
                if (throwable != null) {
                  LOGGER.warn("Failed to clear presence on disposal", throwable);
                }
              }));
    }
  }

  @VisibleForTesting
  synchronized void terminateWithError(final Throwable throwable) {
    if (state != State.TERMINATED && state != State.ERROR) {
      setState(State.ERROR, Event.INTERNAL_TRIGGER);
      emitter.error(throwable);
    }
  }

  private synchronized void onDispose() {
    // Unless the state machine is an already terminal state, directly set state to "terminated", since there's no point
    // in evaluating the state machine when there's no live subscription.
    if (state != State.TERMINATED && state != State.ERROR) {
      setState(State.TERMINATED, Event.INTERNAL_TRIGGER);
    }
    cancelWatch();
    clearPresence();
  }
}
