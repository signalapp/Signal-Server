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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.util.Pair;
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

  /// The maximum number of messages we will fetch per range query operation to avoid excessive memory consumption
  private final int maxMessagesPerScan;

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

  /// A future for refreshing the connected client's presence value at regular intervals.
  @Nullable private ScheduledFuture<?> renewPresenceFuture;

  private final AtomicLong totalRequested = new  AtomicLong(0);
  private final AtomicLong totalEmitted = new AtomicLong(0);

  private static final AtomicInteger NEXT_STREAM_ID = new AtomicInteger();

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDbMessagePublisher.class);

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
      final int maxMessagesPerScan,
      @Nullable final byte[] presenceKey,
      @Nullable final ScheduledExecutorService presenceRenewalExecutorService,
      @Nullable final byte[] messagesAvailableWatchKey,
      @Nullable final BiConsumer<State, State> stateChangeListener) {

    this.database = database;
    this.clock = clock;
    this.beginKeyCursor = beginKeyInclusive;
    this.endKeyExclusive = endKeyExclusive;
    this.maxMessagesPerScan = maxMessagesPerScan;
    this.presenceKey = presenceKey;
    this.messagesAvailableWatchKey = messagesAvailableWatchKey;
    this.terminateOnQueueEmpty = messagesAvailableWatchKey == null;
    this.stateChangeListener = stateChangeListener != null ? stateChangeListener : (_, _) -> {};

    if (!terminateOnQueueEmpty) {
      Objects.requireNonNull(presenceKey);
      Objects.requireNonNull(presenceRenewalExecutorService);
    }

    this.messagePublisher = Mono.fromFuture(() -> {
          if (!terminateOnQueueEmpty) {
            // Establish presence before attempting to fetch messages. We must establish presence for watches to work as
            // expected. If we fail to establish presence, then this non-terminating stream will never learn about new
            // messages. If establishing presence throws an exception, then the whole publisher will terminate with that
            // exception.
            return database.runAsync(transaction -> {
                  setPresence(transaction);
                  return CompletableFuture.completedFuture(null);
                })
                .thenRun(() -> startPresenceRenewal(presenceRenewalExecutorService));
          } else {
            return CompletableFuture.completedFuture(null);
          }
        })
        .thenMany(Flux.create(emitter -> {
          this.emitter = emitter;
          emitter.onRequest(n -> {
            final long totalRequestedCount = totalRequested.addAndGet(n);
            if (totalRequestedCount > totalEmitted.get()) {
              transitionStateOnEvent(Event.DEMAND_REQUESTED);
            }
          });
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
      final KeySelector endKeyExclusive,
      final int maxMessagesPerScan) {

    return new FoundationDbMessagePublisher(database,
        clock,
        beginKeyInclusive,
        endKeyExclusive,
        maxMessagesPerScan,
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
      final int maxMessagesPerScan,
      final byte[] presenceKey,
      final ScheduledExecutorService presenceRenewalExecutorService,
      final byte[] messagesAvailableWatchKey) {

    return new FoundationDbMessagePublisher(database,
        clock,
        beginKeyInclusive,
        endKeyExclusive,
        maxMessagesPerScan,
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

  /// Fetch messages using a range query limiting batch size to [#maxMessagesPerScan]. If the query returns fewer than
  /// [#maxMessagesPerScan], emit [Event#FETCHED_ALL_AVAILABLE_MESSAGES]. In the case of an infinite publisher, also set
  /// a watch for new messages. Additionally, the cursor is updated so that we begin fetching from the right key on
  /// subsequent scans
  ///
  /// @return a future of a list of [FoundationDbMessageStreamEntry.Message] with a max size of [#maxMessagesPerScan]
  private CompletableFuture<List<FoundationDbMessageStreamEntry.Message>> getMessagesBatch() {
    return database.runAsync(transaction -> getItemsInRange(transaction, beginKeyCursor, endKeyExclusive, maxMessagesPerScan)
        .thenApply(lastKeyReadAndItems -> {
          if (lastKeyReadAndItems.second().size() < maxMessagesPerScan && !terminateOnQueueEmpty) {
            setWatch(transaction);
          }

          return lastKeyReadAndItems;
        }))
        // Defer any state mutations until after the transaction has been committed. The transaction block can
        // fail/retry, and we don't want to trigger spurious state transitions when that happens.
        .thenApply(lastKeyReadAndItems -> {
          // Set our beginning key to just past the last key read so that we're ready for our next fetch
          lastKeyReadAndItems.first()
              .ifPresent(lastKeyRead -> beginKeyCursor = KeySelector.firstGreaterThan(lastKeyRead));

          if (lastKeyReadAndItems.second().size() < maxMessagesPerScan) {
            transitionStateOnEvent(Event.FETCHED_ALL_AVAILABLE_MESSAGES);
          }

          return lastKeyReadAndItems.second();
        });
  }

  /// Fetch messages in the range between `begin` and `end` limited to a batch size of `maxMessagesPerSccan`
  ///
  /// @param transaction        the FoundationDB transaction in which to perform the read query
  /// @param beginInclusive     the range start key (inclusive)
  /// @param endExclusive       the range end key (exclusive)
  /// @param maxMessagesPerScan maximum number of messages to return in the fetch query
  /// @return the last key read (if there were non-zero number of messages read) and the list of messages read
  private CompletableFuture<Pair<Optional<byte[]>, List<FoundationDbMessageStreamEntry.Message>>> getItemsInRange(
      final Transaction transaction,
      final KeySelector beginInclusive,
      final KeySelector endExclusive,
      final int maxMessagesPerScan) {
    return transaction.getRange(beginInclusive, endExclusive, maxMessagesPerScan, false, StreamingMode.EXACT).asList()
        .thenApply(keyValues -> {
          final Optional<byte[]> lastKeyRead = keyValues.isEmpty()
              ? Optional.empty()
              : Optional.of(keyValues.getLast().getKey());
          final List<FoundationDbMessageStreamEntry.Message> messages = keyValues.stream()
              .map(keyValue -> {
                final Versionstamp versionstamp = FoundationDbMessageStore.getVersionstamp(keyValue.getKey());

                try {
                  return new FoundationDbMessageStreamEntry.Message(versionstamp, MessageProtos.Envelope.parseFrom(keyValue.getValue()));
                } catch (final InvalidProtocolBufferException e) {
                  throw new UncheckedIOException(e);
                }
              })
              .toList();
          return new Pair<>(lastKeyRead, messages);
        });
  }

  /// Fetch and publish messages. Messages are fetched in batches of [#maxMessagesPerScan] to avoid excessive memory
  /// consumption. After each fetch operation, [#beginKeyCursor] is updated to the next key we need to start reading
  /// from. If the fetch operation returns fewer items than the batch size, we infer that we have fetched all available
  /// messages and [Event#FETCHED_ALL_AVAILABLE_MESSAGES] is sent to the state machine. See [#getMessagesBatch()]
  /// for details. Additionally, after we successfully publish the batch of messages, {@link Event#PUBLISHED_MESSAGES}
  /// is emitted. If there's an error while fetching or publishing, [Event#FETCH_OR_PUBLISH_ERROR_OCCURRED] is emitted
  /// instead.
  private void emitMessages() {
    getMessagesBatch()
        .thenAccept(messageStreamEntries -> {
          messageStreamEntries.forEach(emitter::next);
          totalEmitted.addAndGet(messageStreamEntries.size());
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
    // Set a watch that will be triggered when new messages arrive. When it is triggered, we attempt to fetch messages
    // again (if there is demand). When we run out of messages, this method will be called again, setting another watch,
    // and so on, thus achieving a "watch for new messages -> read -> publish" loop.
    watchFuture = transaction.watch(messagesAvailableWatchKey);
    watchFuture.thenRun(() -> transitionStateOnEvent(Event.MESSAGE_AVAILABLE_WATCH_TRIGGERED));
  }

  /// Cancel the watch (if any). Although inactive watches are automatically timed-out, we explicitly cancel when the
  /// subscription is disposed to clean up associated resources and to avoid having too many watches open at once.
  private synchronized void cancelWatch() {
    if (watchFuture != null) {
      watchFuture.cancel(true);
    }
  }

  private synchronized void startPresenceRenewal(final ScheduledExecutorService presenceRenewalExecutorService) {
    final long renewalIntervalMillis =
        FoundationDbMessageStore.PRESENCE_STALE_THRESHOLD.multipliedBy(4).dividedBy(5).toMillis();

    renewPresenceFuture = presenceRenewalExecutorService.scheduleWithFixedDelay(this::renewPresence,
        renewalIntervalMillis,
        renewalIntervalMillis,
        TimeUnit.MILLISECONDS);
  }

  private synchronized void renewPresence() {
    if (state == State.TERMINATED || state == State.ERROR) {
      return;
    }

    database.runAsync(transaction -> {
      setPresence(transaction);
      return CompletableFuture.completedFuture(null);
    })
    .whenComplete((_, throwable) -> {
      if (throwable != null) {
        LOGGER.warn("Failed to renew presence; will terminate publisher", throwable);
        terminateWithError(throwable);
      }
    });
  }

  private void setPresence(final Transaction transaction) {
    transaction.set(presenceKey, FoundationDbMessageStore.getPresenceValue(clock.instant(), streamId));
  }

  @VisibleForTesting
  synchronized void clearPresence() {
    if (renewPresenceFuture != null) {
      renewPresenceFuture.cancel(true);
    }

    if (!terminateOnQueueEmpty) {
      database.runAsync(transaction -> transaction.get(presenceKey).thenAccept(presenceValue -> {
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
          });
    }
  }

  @VisibleForTesting
  synchronized void terminateWithError(final Throwable throwable) {
    if (state != State.TERMINATED && state != State.ERROR) {
      setState(State.ERROR, Event.INTERNAL_TRIGGER);
    }

    emitter.error(throwable);
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
