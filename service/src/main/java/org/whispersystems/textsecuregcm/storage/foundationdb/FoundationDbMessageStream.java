package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.subspace.Subspace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.MessageStream;
import org.whispersystems.textsecuregcm.storage.MessageStreamEntry;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;


/// A [MessageStream] implementation that fetches messages from FoundationDB
public class FoundationDbMessageStream implements MessageStream {

  private final FoundationDbMessageStore foundationDbMessageStore;
  private final AciServiceIdentifier aciServiceIdentifier;
  private final byte deviceId;

  private final Subspace deviceQueueSubspace;
  private final byte[] presenceKey;
  private final byte[] messagesAvailableWatchKey;
  private final Database[] databasesByEpoch;
  private final MessageGuidCodec messageGuidCodec;
  /// The maximum number of messages we will fetch per range query operation to avoid excessive memory consumption
  private final int maxMessagesPerScan;
  private final Flow.Publisher<MessageStreamEntry> messageStreamPublisher;
  private final Runnable doAfterCleanup;
  private final ScheduledExecutorService presenceRenewalExecutorService;
  private final Clock clock;

  private final Counter messageReadCounter =
      Metrics.counter(name(FoundationDbMessageStream.class, "messagesRead"));

  private final Counter messageAcknowledgedCounter =
      Metrics.counter(name(FoundationDbMessageStream.class, "messagesAcknowledged"));

  private final Counter staleEphemeralMessagesCounter =
      Metrics.counter(name(FoundationDbMessageStream.class, "staleEphemeralMessages"));

  static final int DEFAULT_MAX_MESSAGES_PER_SCAN = 100;

  private static final Comparator<FoundationDbMessageStreamEntry.Message> STREAM_ENTRY_TIMESTAMP_COMPARATOR =
      Comparator.comparingLong(streamEntry -> streamEntry.partialEnvelope().getServerTimestamp());

  FoundationDbMessageStream(final FoundationDbMessageStore foundationDbMessageStore,
      final AciServiceIdentifier aciServiceIdentifier,
      final byte deviceId,
      final Database[] databasesByEpoch,
      final MessageGuidCodec messageGuidCodec,
      final int maxMessagesPerScan,
      final Runnable doAfterCleanup,
      final ScheduledExecutorService presenceRenewalExecutorService,
      final Clock clock) {
    this.foundationDbMessageStore = foundationDbMessageStore;
    this.aciServiceIdentifier = aciServiceIdentifier;
    this.deviceId = deviceId;
    this.deviceQueueSubspace = FoundationDbMessageStore.getDeviceQueueSubspace(aciServiceIdentifier, deviceId);
    this.presenceKey = FoundationDbMessageStore.getPresenceKey(aciServiceIdentifier, deviceId);
    this.messagesAvailableWatchKey = FoundationDbMessageStore.getMessagesAvailableWatchKey(aciServiceIdentifier);
    this.databasesByEpoch = databasesByEpoch;
    this.messageGuidCodec = messageGuidCodec;
    this.maxMessagesPerScan = maxMessagesPerScan;
    this.messageStreamPublisher = JdkFlowAdapter.publisherToFlowPublisher(createMessagePublisher());
    this.doAfterCleanup = doAfterCleanup;
    this.presenceRenewalExecutorService = presenceRenewalExecutorService;
    this.clock = clock;
  }

  @Override
  public Flow.Publisher<MessageStreamEntry> getMessages() {
    return this.messageStreamPublisher;
  }

  public Flux<MessageStreamEntry> getFiniteMessageStream() {
    // This may seem like an odd construction since it looks like we could also just do `Flux#fromArray`, but
    // `Flux#fromArray` cannot handle `null` elements
    final List<Database> databases = Arrays.stream(databasesByEpoch).filter(Objects::nonNull).distinct().toList();

    return Flux.fromIterable(databases)
        .flatMap(database -> Mono.fromFuture(getEndOfQueueKeyExclusive(database))
            .map(maybeEndOfQueueKeyExclusive -> Tuples.of(database, maybeEndOfQueueKeyExclusive)))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .flatMapMany(endOfQueueKeysByDatabase -> {
          @SuppressWarnings("unchecked") final Flux<FoundationDbMessageStreamEntry.Message>[] finitePublishers =
              endOfQueueKeysByDatabase.entrySet().stream()
                  .map(entry -> {
                    final Database database = entry.getKey();
                    final Optional<KeySelector> maybeEndOfQueueKeyExclusive = entry.getValue();

                    return maybeEndOfQueueKeyExclusive
                        .map(endOfQueueKeyExclusive -> FoundationDbMessagePublisher.createFinitePublisher(
                                database,
                                clock,
                                KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin),
                                endOfQueueKeyExclusive,
                                maxMessagesPerScan)
                            .getMessages())
                        .orElseGet(Flux::empty);
                  })
                  .toArray(Flux[]::new);

          return Flux.concat(
                  Flux.mergeComparing(maxMessagesPerScan, STREAM_ENTRY_TIMESTAMP_COMPARATOR, finitePublishers)
                      .doOnNext(_ -> messageReadCounter.increment()),
                  Mono.just(new FoundationDbMessageStreamEntry.QueueEmpty()))
              .map(fdbMessageStreamEntry -> fdbMessageStreamEntry.toMessageStreamEntry(messageGuidCodec));
        });
  }

  /// Create a message publisher
  ///
  /// @return a Flux of {@link MessageStreamEntry} fetched from FoundationDB
  /// @implNote turns the stream of [FoundationDbMessageStreamEntry] into [MessageStreamEntry], but taps into the stream
  /// first to keep track of versionstamps sent to the client.
  private Flux<MessageStreamEntry> createMessagePublisher() {
    return createFoundationDbMessagePublisher()
        .map(fdbMessageStreamEntry -> fdbMessageStreamEntry.toMessageStreamEntry(messageGuidCodec))
        .doOnNext(messageStreamEntry -> {
          if (messageStreamEntry instanceof MessageStreamEntry.Envelope) {
            messageReadCounter.increment();
          }
        })
        .doFinally(_ -> doAfterCleanup.run());
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
    // This may seem like an odd construction since it looks like we could also just do `Flux#fromArray`, but
    // `Flux#fromArray` cannot handle `null` elements
    final List<Database> databases = Arrays.stream(databasesByEpoch).filter(Objects::nonNull).distinct().toList();

    return Flux.fromIterable(databases)
        .flatMap(database -> Mono.fromFuture(getEndOfQueueKeyExclusive(database))
            .map(maybeEndOfQueueKeyExclusive -> Tuples.of(database, maybeEndOfQueueKeyExclusive)))
        .collectMap(Tuple2::getT1, Tuple2::getT2)
        .flatMapMany(endOfQueueKeysByDatabase -> {
          @SuppressWarnings("unchecked") final Flux<FoundationDbMessageStreamEntry.Message>[] finitePublishers =
              endOfQueueKeysByDatabase.entrySet().stream()
                  .map(entry -> {
                    final Database database = entry.getKey();
                    final Optional<KeySelector> maybeEndOfQueueKeyExclusive = entry.getValue();

                    return maybeEndOfQueueKeyExclusive
                        .map(endOfQueueKeyExclusive -> FoundationDbMessagePublisher.createFinitePublisher(
                                database,
                                clock,
                                KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin),
                                endOfQueueKeyExclusive,
                                maxMessagesPerScan)
                            .getMessages())
                        .orElseGet(Flux::empty)
                        .handle((fdbMessageStreamEntry, sink) -> {
                          final MessageProtos.Envelope partialEnvelope = fdbMessageStreamEntry.partialEnvelope();

                          // Ephemeral messages from the finite stream are considered stale and automatically discarded
                          if (partialEnvelope.getEphemeral()) {
                            foundationDbMessageStore.delete(aciServiceIdentifier, deviceId, fdbMessageStreamEntry.versionstamp());

                            staleEphemeralMessagesCounter.increment();
                            return;
                          }
                          sink.next(fdbMessageStreamEntry);
                        });
                  })
                  .toArray(Flux[]::new);

          @SuppressWarnings("unchecked") final Flux<FoundationDbMessageStreamEntry.Message>[] infinitePublishers =
              endOfQueueKeysByDatabase.entrySet().stream()
                  .map(entry -> {
                    final Database database = entry.getKey();
                    final Optional<KeySelector> maybeEndOfQueueKeyExclusive = entry.getValue();

                    final KeySelector infinitePublisherBeginKey = maybeEndOfQueueKeyExclusive
                        .orElseGet(() -> KeySelector.firstGreaterOrEqual(deviceQueueSubspace.range().begin));

                    return FoundationDbMessagePublisher.createInfinitePublisher(
                        database,
                        clock,
                        infinitePublisherBeginKey,
                        KeySelector.firstGreaterThan(deviceQueueSubspace.range().end),
                        maxMessagesPerScan,
                        presenceKey,
                        presenceRenewalExecutorService,
                        messagesAvailableWatchKey).getMessages();
                  })
                  .toArray(Flux[]::new);

          return Flux.concat(
              Flux.mergeComparing(maxMessagesPerScan, STREAM_ENTRY_TIMESTAMP_COMPARATOR, finitePublishers),
              Mono.just(new FoundationDbMessageStreamEntry.QueueEmpty()),

              // Note that we use `mergePriority` instead of `mergeComparing` for the "live"/non-terminating publishers
              // because `mergePriority` sorts messages _as they arrive._ If we used `mergeComparing` for the live
              // streams and one of the streams had no new messages (which will be true most of the time), then the
              // merged publisher would never emit any signals because it'd be waiting to have something to compare
              // against. This does mean that we risk some slightly out-of-order messages in the exceedingly rare cases
              // where messages arrive at different servers while somebody is connected and a migration is in progress,
              // but that should be (again) exceedingly rare and also minimally-disruptive (i.e. it would self-correct
              // so quickly that end users would likely never even notice).
              Flux.mergePriority(maxMessagesPerScan, STREAM_ENTRY_TIMESTAMP_COMPARATOR, infinitePublishers));
        });
  }

  /// Gets a [KeySelector] for the first key greater than the current greatest key in the device queue. This allows us
  /// to query keys up to and including the greatest key, and sets us up to begin reading from the next key in a
  /// subsequent scan.
  ///
  /// @return a [KeySelector] for the first key greater than the current greatest key in the device queue.
  private CompletableFuture<Optional<KeySelector>> getEndOfQueueKeyExclusive(final Database database) {
    return database.runAsync(transaction ->
            transaction.getRange(deviceQueueSubspace.range(), 1, true, StreamingMode.EXACT).asList())
        .thenApply(items -> {
          if (items.isEmpty()) {
            return Optional.empty();
          }
          assert items.size() == 1;
          return Optional.of(KeySelector.firstGreaterThan(items.getFirst().getKey()));
        });
  }

  @Override
  public CompletableFuture<Void> acknowledgeMessage(final UUID messageGuid, final long serverTimestamp) {
    messageAcknowledgedCounter.increment();

    return foundationDbMessageStore.delete(aciServiceIdentifier, deviceId, messageGuid);
  }
}
