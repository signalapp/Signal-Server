package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.MessageStream;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/// A [MessageStream] implementation that fetches messages from FoundationDB
public class FoundationDbMessageStream implements MessageStream {

  private final Subspace deviceQueueSubspace;
  private final byte[] messagesAvailableWatchKey;
  private final Database database;
  private final MessageGuidCodec messageGuidCodec;
  /// The maximum number of messages we will fetch per range query operation to avoid excessive memory consumption
  private final int maxMessagesPerScan;
  private final Flow.Publisher<MessageStreamEntry> messageStreamPublisher;
  private final Runnable doAfterCleanup;
  private final Clock clock;

  private final AcknowledgedMessageBuffer acknowledgedMessageBuffer;

  @VisibleForTesting
  static final Duration MAX_EPHEMERAL_MESSAGE_DELAY = Duration.ofSeconds(10);
  static final int DEFAULT_MAX_MESSAGES_PER_SCAN = 1024;
  @VisibleForTesting
  static final int DEFAULT_MAX_UNACKNOWLEDGED_MESSAGES = 16_384;

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDbMessageStream.class);

  FoundationDbMessageStream(final Subspace deviceQueueSubspace,
      final byte[] messagesAvailableWatchKey,
      final Database database,
      final MessageGuidCodec messageGuidCodec,
      final int maxMessagesPerScan,
      final int maxUnacknowledgedMessages,
      final Runnable doAfterCleanup,
      final Clock clock) {
    this.deviceQueueSubspace = deviceQueueSubspace;
    this.messagesAvailableWatchKey = messagesAvailableWatchKey;
    this.database = database;
    this.messageGuidCodec = messageGuidCodec;
    this.maxMessagesPerScan = maxMessagesPerScan;
    this.clock = clock;
    this.messageStreamPublisher = JdkFlowAdapter.publisherToFlowPublisher(createMessagePublisher());
    this.doAfterCleanup = doAfterCleanup;

    this.acknowledgedMessageBuffer = new AcknowledgedMessageBuffer(maxUnacknowledgedMessages);
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return this.messageStreamPublisher;
  }

  /// Create a message publisher
  ///
  /// @return a Flux of {@link MessageStreamEntry} fetched from FoundationDB
  /// @implNote turns the stream of [FoundationDbMessageStreamEntry] into [MessageStreamEntry], but taps into the stream
  /// first to keep track of versionstamps sent to the client.
  private Flux<MessageStreamEntry> createMessagePublisher() {
    final long earliestAllowedEphemeralTimestamp = clock.instant().minus(MAX_EPHEMERAL_MESSAGE_DELAY).toEpochMilli();
    return createFoundationDbMessagePublisher()
        .<FoundationDbMessageStreamEntry>handle((messageStreamEntry, sink) -> {
          if (messageStreamEntry instanceof final FoundationDbMessageStreamEntry.Message message) {
            try {
              acknowledgedMessageBuffer.addUnacknowledgedMessage(message.versionstamp());
            } catch (final TooManyUnacknowledgedMessagesException e) {
              sink.error(e);
              return;
            }
          }

          sink.next(messageStreamEntry);
        })
        .map(fdbMessageStreamEntry -> fdbMessageStreamEntry.toMessageStreamEntry(messageGuidCodec))
        .<MessageStreamEntry>handle((messageStreamEntry, sink) -> {
          if (messageStreamEntry instanceof MessageStreamEntry.Envelope(MessageProtos.Envelope message)
              && isStaleEphemeralMessage(message, earliestAllowedEphemeralTimestamp)) {
            acknowledgeMessage(message);
            return;
          }
          sink.next(messageStreamEntry);
        })
        .doFinally(_ -> flushAllAcknowledgedMessages().thenRun(doAfterCleanup));
  }

  /// Create a message publisher that fetches messages from FoundationDB
  ///
  /// @return a Flux of [FoundationDbMessageStreamEntry] fetched from FoundationDB
  /// @implNote The message publisher is stitched together by concatenating:
  /// 1. **A finite message publisher**: On initial request, we record the current end-of-queue key in the device mailbox.
  ///    Then, we fetch all messages in order until the recorded key and finally complete the stream
  /// 2. **A queue-empty signal** is emitted
  /// 3. **An infinite message publisher**: We start reading from where the finite publisher left off. When all messages
  ///    are read, we wait for new messages, publish them, then wait again in a loop forever (until the flux is canceled
  ///    explicitly or due to an error). This is accomplished by setting a FoundationDB [watch](https://github.com/apple/foundationdb/wiki/An-Overview-how-Watches-Work)
  ///    on [#messagesAvailableWatchKey] which is updated when a new message is available.
  ///    See [FoundationDbMessageStore] for more details on the message insert process.
  private Flux<FoundationDbMessageStreamEntry> createFoundationDbMessagePublisher() {
    return Mono.fromFuture(this::getEndOfQueueKeyExclusive)
        .flatMapMany(maybeEndOfQueueKeyExclusive -> {
          final Flux<FoundationDbMessageStreamEntry.Message> finitePublisher = maybeEndOfQueueKeyExclusive
              .map(endOfQueueKeyExclusive -> FoundationDbMessagePublisher.createFinitePublisher(
                  KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin),
                  endOfQueueKeyExclusive, database, maxMessagesPerScan, this::clearAcknowledgedMessages).getMessages())
              .orElseGet(Flux::empty);
          final KeySelector infinitePublisherBeginKey = maybeEndOfQueueKeyExclusive.orElseGet(
              () -> KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin));
          final Flux<FoundationDbMessageStreamEntry.Message> infinitePublisher = FoundationDbMessagePublisher.createInfinitePublisher(
              infinitePublisherBeginKey, KeySelector.firstGreaterThan(deviceQueueSubspace.range().end),
              database, maxMessagesPerScan, messagesAvailableWatchKey, this::clearAcknowledgedMessages).getMessages();
          return Flux.concat(
              finitePublisher,
              Mono.just(new FoundationDbMessageStreamEntry.QueueEmpty()),
              infinitePublisher
          );
        });

  }

  /// Gets a [KeySelector] for the first key greater than the current greatest key in the device queue. This allows us
  /// to query keys up to and including the greatest key, and sets us up to begin reading from the next key in a
  /// subsequent scan.
  ///
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
    acknowledgedMessageBuffer.acknowledgeMessage(
        messageGuidCodec.decodeMessageGuid(UUIDUtil.fromByteString(message.getServerGuidBinary())));

    return CompletableFuture.completedFuture(null);
  }

  /// Clear the versionstamp range (startInclusive, endInclusive) in a single FoundationDB operation.
  ///
  /// @param transaction    The FoundationDB transaction in which to perform the range clear
  /// @param startInclusive The starting versionstamp of the range to be cleared (inclusive)
  /// @param endInclusive   The ending versionstamp of the range to be cleared (inclusive)
  private void clearRange(final Transaction transaction, final Versionstamp startInclusive, final Versionstamp endInclusive) {
    final byte[] startKeyInclusive = deviceQueueSubspace.pack(Tuple.from(startInclusive));
    final byte[] endKeyExclusive = ByteArrayUtil.keyAfter(deviceQueueSubspace.pack(Tuple.from(endInclusive)));
    transaction.clear(startKeyInclusive, endKeyExclusive);
  }

  /// Clear all outstanding acknowledged messages. Called when the stream ends
  private CompletableFuture<Void> flushAllAcknowledgedMessages() {
    final Consumer<Transaction> clearAllAcknowlegedMessagedConsumer = clearAcknowledgedMessages();
    return database.runAsync(transaction -> {
          clearAllAcknowlegedMessagedConsumer.accept(transaction);
          return CompletableFuture.completedFuture((Void) null);
        })
        .whenComplete((_, throwable) -> {
          if (throwable != null) {
            LOGGER.warn("Failed to clear acknowledged messages", throwable);
          }
        });
  }

  private synchronized Consumer<Transaction> clearAcknowledgedMessages() {
    final List<Pair<Versionstamp, Versionstamp>> flushableRanges = acknowledgedMessageBuffer.takeFlushableRanges();

    return transaction -> flushableRanges.forEach(range -> clearRange(transaction, range.first(), range.second()));
  }

  private static boolean isStaleEphemeralMessage(final MessageProtos.Envelope message,
      final long earliestAllowableTimestamp) {
    return message.getEphemeral() && message.getClientTimestamp() < earliestAllowableTimestamp;
  }
}
