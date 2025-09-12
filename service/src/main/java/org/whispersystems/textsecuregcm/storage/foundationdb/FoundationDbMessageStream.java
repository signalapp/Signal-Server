package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.subspace.Subspace;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.MessageStream;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/// A [MessageStream] implementation that fetches messages from FoundationDB
public class FoundationDbMessageStream implements MessageStream {

  private final Subspace deviceQueueSubspace;
  private final byte[] messagesAvailableWatchKey;
  private final Database database;
  /// The maximum number of messages we will fetch per range query operation to avoid excessive memory consumption
  private final int maxMessagesPerScan;
  private final Flow.Publisher<MessageStreamEntry> messageStreamPublisher;

  static final int DEFAULT_MAX_MESSAGES_PER_SCAN = 1024;

  FoundationDbMessageStream(final Subspace deviceQueueSubspace, final byte[] messagesAvailableWatchKey,
      final Database database, final int maxMessagesPerScan) {
    this.deviceQueueSubspace = deviceQueueSubspace;
    this.messagesAvailableWatchKey = messagesAvailableWatchKey;
    this.database = database;
    this.maxMessagesPerScan = maxMessagesPerScan;
    this.messageStreamPublisher = JdkFlowAdapter.publisherToFlowPublisher(createMessagePublisher());
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return this.messageStreamPublisher;
  }

    /// Create a message publisher
    ///
    /// @return a Flux of {@link MessageStreamEntry} fetched from FoundationDB
    /// @implNote The message publisher is stitched together by concatenating:
    /// 1. **A finite message publisher**: On initial request, we record the current end-of-queue key in the device mailbox.
    ///    Then, we fetch all messages in order until the recorded key and finally complete the stream
    /// 2. **A queue-empty signal** is emitted
    /// 3. **An infinite message publisher**: We start reading from where the finite publisher left off. When all messages
    ///    are read, we wait for new messages, publish them, then wait again in a loop forever (until the flux is canceled
    ///    explicitly or due to an error). This is accomplished by setting a FoundationDB [watch](https://github.com/apple/foundationdb/wiki/An-Overview-how-Watches-Work)
    ///    on [#messagesAvailableWatchKey] which is updated when a new message is available.
    ///    See [FoundationDbMessageStore] for more details on the message insert process.
    private Flux<MessageStreamEntry> createMessagePublisher() {
      return Mono.fromFuture(this::getEndOfQueueKeyExclusive)
        .flatMapMany(maybeEndOfQueueKeyExclusive -> {
          final Flux<MessageStreamEntry.Envelope> finitePublisher = maybeEndOfQueueKeyExclusive
              .map(endOfQueueKeyExclusive -> FoundationDbMessagePublisher.createFinitePublisher(
                  KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin),
                  endOfQueueKeyExclusive, database, maxMessagesPerScan).getMessages())
              .orElseGet(Flux::empty);
          final KeySelector infinitePublisherBeginKey = maybeEndOfQueueKeyExclusive.orElseGet(
              () -> KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin));
          final Flux<MessageStreamEntry.Envelope> infinitePublisher = FoundationDbMessagePublisher.createInfinitePublisher(
              infinitePublisherBeginKey, KeySelector.firstGreaterThan(deviceQueueSubspace.range().end),
              database, maxMessagesPerScan, messagesAvailableWatchKey).getMessages();
          return Flux.concat(
              finitePublisher,
              Mono.just(new MessageStreamEntry.QueueEmpty()),
              infinitePublisher
          );
        });
  }

    /// Gets a [KeySelector] for the first key greater than the current greatest key in the device queue. This allows
    /// us to query keys up to and including the greatest key, and sets us up to begin reading from the next key in
    /// a subsequent scan.
    /// @return a [KeySelector] for the first key greater than the current greatest key in the device queue.
    private CompletableFuture<Optional<KeySelector>> getEndOfQueueKeyExclusive() {
    return database.runAsync(
        transaction -> transaction.getRange(deviceQueueSubspace.range(), 1, true, StreamingMode.EXACT).asList()
            .thenApply(items -> {
              if (items.isEmpty()) {
                return Optional.empty();
              }
              assert items.size() == 1;
              return Optional.of(KeySelector.firstGreaterThan(items.getFirst().getKey()));
            }));
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final MessageProtos.Envelope message) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
