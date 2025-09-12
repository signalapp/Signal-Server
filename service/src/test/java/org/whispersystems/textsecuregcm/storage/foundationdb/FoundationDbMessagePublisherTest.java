package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import reactor.test.StepVerifier;

/// NOTE: most of the happy-path test cases are already covered in {@link FoundationDbMessageStoreTest}, this test
/// mostly exercises edge-cases and error handling that are hard to test without mocks
class FoundationDbMessagePublisherTest {

  private final Range subspaceRange = new Range(new byte[]{(byte) 0}, new byte[]{(byte) 100});
  private final byte[] messagesAvailableWatchKey = new byte[]{(byte) 42};

  private Database database;
  private List<FoundationDbMessagePublisher.State> stateTransitions;

  @BeforeEach
  void setUp() {
    database = mock(Database.class);
    stateTransitions = new ArrayList<>();
  }

  @Test
  void finitePublisherMultipleBatches() {
    final MessageProtos.Envelope message1 = FoundationDbMessageStoreTest.generateRandomMessage(false);
    final MessageProtos.Envelope message2 = FoundationDbMessageStoreTest.generateRandomMessage(false);
    final MessageProtos.Envelope message3 = FoundationDbMessageStoreTest.generateRandomMessage(false);

    final KeyValue keyValue1 = mockKeyValue((byte) 5, message1);
    final KeyValue keyValue2 = mockKeyValue((byte) 6, message2);
    final KeyValue keyValue3 = mockKeyValue((byte) 7, message3);

    final AsyncIterable<KeyValue> batch1 = mock(AsyncIterable.class);
    when(batch1.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue1, keyValue2)));

    final AsyncIterable<KeyValue> batch2 = mock(AsyncIterable.class);
    when(batch2.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue3)));

    final Transaction transaction = mock(Transaction.class);
    when(transaction.getRange(any(KeySelector.class), any(KeySelector.class), anyInt(), anyBoolean(), any(
        StreamingMode.class)))
        .thenReturn(batch1)
        .thenReturn(batch2);

    when(database.runAsync(any(Function.class))).thenAnswer(
        (Answer<CompletableFuture<List<? extends MessageStreamEntry>>>) invocationOnMock -> {
          final Function<Transaction, CompletableFuture<List<? extends MessageStreamEntry>>> f = invocationOnMock.getArgument(
              0);
          return f.apply(transaction);
        });

    final FoundationDbMessagePublisher finitePublisher = new FoundationDbMessagePublisher(
        KeySelector.firstGreaterOrEqual(subspaceRange.begin),
        KeySelector.firstGreaterOrEqual(new byte[]{(byte) 10}),
        database,
        2, // With 3 messages and batch size set to 2, we'll need to grab 2 batches.
        null,
        (oldState, newState) -> stateTransitions.add(newState)
    );

    StepVerifier.create(finitePublisher.getMessages())
        .expectNext(new MessageStreamEntry.Envelope(message1))
        .expectNext(new MessageStreamEntry.Envelope(message2))
        .expectNext(new MessageStreamEntry.Envelope(message3))
        .verifyComplete();

    assertEquals(List.of(
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 1
            FoundationDbMessagePublisher.State.MESSAGES_AVAILABLE,
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 2
            FoundationDbMessagePublisher.State.QUEUE_EMPTY,
            FoundationDbMessagePublisher.State.TERMINATED
        ),
        stateTransitions
    );

  }

  @Test
  @SuppressWarnings({"unchecked", "resource"})
  void infinitePublisher() {
    final MessageProtos.Envelope message1 = FoundationDbMessageStoreTest.generateRandomMessage(false);
    final MessageProtos.Envelope message2 = FoundationDbMessageStoreTest.generateRandomMessage(false);
    final MessageProtos.Envelope message3 = FoundationDbMessageStoreTest.generateRandomMessage(false);

    final KeyValue keyValue1 = mockKeyValue((byte) 5, message1);
    final KeyValue keyValue2 = mockKeyValue((byte) 6, message2);
    final KeyValue keyValue3 = mockKeyValue((byte) 7, message3);

    // Each message in a separate batch; the latter 2 when the messages available watch triggers
    final AsyncIterable<KeyValue> batch1 = mock(AsyncIterable.class);
    when(batch1.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue1)));

    final AsyncIterable<KeyValue> batch2 = mock(AsyncIterable.class);
    when(batch2.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue2)));

    final AsyncIterable<KeyValue> batch3 = mock(AsyncIterable.class);
    when(batch3.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue3)));

    final Transaction transaction = mock(Transaction.class);
    when(transaction.getRange(any(KeySelector.class), any(KeySelector.class), anyInt(), anyBoolean(), any(
        StreamingMode.class)))
        .thenReturn(batch1)
        .thenReturn(batch2)
        .thenReturn(batch3);

    when(database.runAsync(any(Function.class))).thenAnswer(
        (Answer<CompletableFuture<List<? extends MessageStreamEntry>>>) invocationOnMock -> {
          final Function<Transaction, CompletableFuture<List<? extends MessageStreamEntry>>> f = invocationOnMock.getArgument(
              0);
          return f.apply(transaction);
        });

    final CompletableFuture<Void> watchFuture1 = new CompletableFuture<>();
    final CompletableFuture<Void> watchFuture2 = new CompletableFuture<>();
    final CompletableFuture<Void> watchFuture3 = new CompletableFuture<>(); // this one will not be completed
    when(transaction.watch(messagesAvailableWatchKey))
        .thenReturn(watchFuture1)
        .thenReturn(watchFuture2)
        .thenReturn(watchFuture3);

    final FoundationDbMessagePublisher infinitePublisher = new FoundationDbMessagePublisher(
        KeySelector.firstGreaterOrEqual(subspaceRange.begin),
        KeySelector.firstGreaterOrEqual(new byte[]{(byte) 10}),
        database,
        2,
        messagesAvailableWatchKey,
        (oldState, newState) -> {
          stateTransitions.add(newState);
          if (newState == FoundationDbMessagePublisher.State.AWAITING_NEW_MESSAGES) {
            // We trigger the watch in "awaiting new messages" state to simulate a "live stream" of changes
            final long numAwaitingStates = stateTransitions.stream()
                .filter(s -> s == FoundationDbMessagePublisher.State.AWAITING_NEW_MESSAGES)
                .count();
            // We simulate the arrival of 2 new messages, so we don't complete the 3rd watch.
            if (numAwaitingStates == 1) {
              watchFuture1.complete(null);
            } else if (numAwaitingStates == 2) {
              watchFuture2.complete(null);
            }
          }
        }
    );

    StepVerifier.create(infinitePublisher.getMessages())
        .expectNext(new MessageStreamEntry.Envelope(message1))
        .expectNext(new MessageStreamEntry.Envelope(message2))
        .expectNext(new MessageStreamEntry.Envelope(message3))
        .verifyTimeout(Duration.ofSeconds(1));

    assertEquals(List.of(
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 1
            FoundationDbMessagePublisher.State.QUEUE_EMPTY,
            FoundationDbMessagePublisher.State.AWAITING_NEW_MESSAGES,
            FoundationDbMessagePublisher.State.MESSAGES_AVAILABLE, // Watch 1 triggered
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 2
            FoundationDbMessagePublisher.State.QUEUE_EMPTY,
            FoundationDbMessagePublisher.State.AWAITING_NEW_MESSAGES,
            FoundationDbMessagePublisher.State.MESSAGES_AVAILABLE, // Watch 2 triggered
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 3
            FoundationDbMessagePublisher.State.QUEUE_EMPTY,
            FoundationDbMessagePublisher.State.AWAITING_NEW_MESSAGES,
            FoundationDbMessagePublisher.State.TERMINATED
        ),
        stateTransitions
    );
    assertTrue(watchFuture3.isCancelled());
  }

  @Test
  void messageAvailableWatchSignalBuffered() {
    final MessageProtos.Envelope message1 = FoundationDbMessageStoreTest.generateRandomMessage(false);
    final MessageProtos.Envelope message2 = FoundationDbMessageStoreTest.generateRandomMessage(false);

    final KeyValue keyValue1 = mockKeyValue((byte) 5, message1);
    final KeyValue keyValue2 = mockKeyValue((byte) 6, message2);

    final AsyncIterable<KeyValue> batch1 = mock(AsyncIterable.class);
    when(batch1.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue1)));

    final AsyncIterable<KeyValue> batch2 = mock(AsyncIterable.class);
    when(batch2.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue2)));

    final Transaction transaction = mock(Transaction.class);
    when(transaction.getRange(any(KeySelector.class), any(KeySelector.class), anyInt(), anyBoolean(), any(
        StreamingMode.class)))
        .thenReturn(batch1)
        .thenReturn(batch2);

    when(database.runAsync(any(Function.class))).thenAnswer(
        (Answer<CompletableFuture<List<? extends MessageStreamEntry>>>) invocationOnMock -> {
          final Function<Transaction, CompletableFuture<List<? extends MessageStreamEntry>>> f = invocationOnMock.getArgument(
              0);
          return f.apply(transaction);
        });

    final CompletableFuture<Void> watchFuture1 = new CompletableFuture<>();
    final CompletableFuture<Void> watchFuture2 = new CompletableFuture<>(); // this one will not be completed
    when(transaction.watch(messagesAvailableWatchKey))
        .thenReturn(watchFuture1)
        .thenReturn(watchFuture2);

    final FoundationDbMessagePublisher infinitePublisher = new FoundationDbMessagePublisher(
        KeySelector.firstGreaterOrEqual(subspaceRange.begin),
        KeySelector.firstGreaterOrEqual(new byte[]{(byte) 10}),
        database,
        2,
        messagesAvailableWatchKey,
        (oldState, newState) -> {
          stateTransitions.add(newState);
          // Simulate an edge case where the messages available watch could trigger right after queue empty, but before
          // all messages have been published. This exploits the fact that the state transitions and the listener calls
          // are synchronized, and for an already completed future, the callback runs in the calling thread, which
          // ensures that the watch is triggered before the messages are published.
          if (newState == FoundationDbMessagePublisher.State.QUEUE_EMPTY &&
              stateTransitions.stream()
                  .filter(s -> s == FoundationDbMessagePublisher.State.QUEUE_EMPTY)
                  .count() == 1) {
            watchFuture1.complete(null);
          }
        }
    );

    StepVerifier.create(infinitePublisher.getMessages())
        .expectNext(new MessageStreamEntry.Envelope(message1))
        .expectNext(new MessageStreamEntry.Envelope(message2))
        .verifyTimeout(Duration.ofSeconds(1));

    assertEquals(List.of(
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 1
            FoundationDbMessagePublisher.State.QUEUE_EMPTY,
            FoundationDbMessagePublisher.State.MESSAGE_AVAILABLE_SIGNAL_BUFFERED,
            // Watch triggered but messages aren't published yet
            FoundationDbMessagePublisher.State.MESSAGES_AVAILABLE,
            FoundationDbMessagePublisher.State.FETCHING_MESSAGES, // Batch 2
            FoundationDbMessagePublisher.State.QUEUE_EMPTY,
            FoundationDbMessagePublisher.State.AWAITING_NEW_MESSAGES,
            FoundationDbMessagePublisher.State.TERMINATED
        ),
        stateTransitions
    );
    assertTrue(watchFuture2.isCancelled());
  }

  @Test
  @SuppressWarnings({"unchecked", "resource"})
  void watchCanceledOnSubscriptionCancel() {
    final FoundationDbMessagePublisher infinitePublisher = FoundationDbMessagePublisher.createInfinitePublisher(
        KeySelector.firstGreaterOrEqual(subspaceRange.begin),
        KeySelector.firstGreaterThan(subspaceRange.end),
        database,
        100,
        messagesAvailableWatchKey);
    final MessageProtos.Envelope message = FoundationDbMessageStoreTest.generateRandomMessage(false);
    final Transaction transaction = mock(Transaction.class);
    final KeyValue keyValue = mock(KeyValue.class);
    when(keyValue.getKey()).thenReturn(new byte[]{(byte) 5});
    when(keyValue.getValue()).thenReturn(message.toByteArray());
    final AsyncIterable<KeyValue> asyncIterable = mock(AsyncIterable.class);
    when(asyncIterable.asList()).thenReturn(CompletableFuture.completedFuture(List.of(keyValue)));
    when(transaction.getRange(any(KeySelector.class), any(KeySelector.class), anyInt(), anyBoolean(), any(
        StreamingMode.class)))
        .thenReturn(asyncIterable);
    final CompletableFuture<Void> watchFuture = mock(CompletableFuture.class);
    when(transaction.watch(messagesAvailableWatchKey)).thenReturn(watchFuture);

    when(database.runAsync(any(Function.class))).thenAnswer(
        (Answer<CompletableFuture<List<? extends MessageStreamEntry>>>) invocationOnMock -> {
          final Function<Transaction, CompletableFuture<List<? extends MessageStreamEntry>>> f = invocationOnMock.getArgument(
              0);
          return f.apply(transaction);
        });
    StepVerifier.create(infinitePublisher.getMessages())
        .expectNext(new MessageStreamEntry.Envelope(message))
        .thenCancel()
        .verify(Duration.ofMillis(100));

    verify(watchFuture).cancel(true);
  }

  private KeyValue mockKeyValue(final byte key, final MessageProtos.Envelope message) {
    final KeyValue keyValue = mock(KeyValue.class);
    when(keyValue.getKey()).thenReturn(new byte[]{key});
    when(keyValue.getValue()).thenReturn(message.toByteArray());
    return keyValue;
  }

}
