/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import io.micrometer.core.instrument.Timer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.signal.chat.messages.GetMessagesResponse;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MessageMetrics;
import org.whispersystems.textsecuregcm.util.ClosableEpoch;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/// Track messages while they are in-flight so they can later be retrieved for acknowledgement, as well as when
/// acknowledgement completes.
///
/// # Example Usage
/// ```java
/// PendingAcknowledgement tracker = new PendingAcknowledgementTracker(...);
/// tracker.addUnacknowledgedEnvelope(envelope);
/// sendEnvelopeToDevice(envelope);
///
/// // Later, when the device acklowedges the envelope
///  UnacknowledgedEnvelope envelope = takeUnacknowledgedEnvelope(uuid);
///  removeFromStorage(envelope);
///  envelope.handleMessageAcknowledged();
/// ```
class PendingAcknowledgementTracker {

  private final ConcurrentHashMap<UUID, UnacknowledgedEnvelope> unacknowledgedEnvelopes = new ConcurrentHashMap<>();
  private final AtomicLong sentMessageCounter = new AtomicLong();

  private final Sinks.One<GetMessagesResponse> queueDrained = Sinks.one();
  private final ClosableEpoch queueDrainEpoch = new ClosableEpoch(() ->
      queueDrained.tryEmitValue(MessageDispatcher.QUEUE_EMPTY_RESPONSE));
  private final Timer.Sample queueDrainStart = Timer.start();

  private final MessageMetrics messageMetrics;
  @Nullable private final UserAgent userAgent;

  PendingAcknowledgementTracker(final MessageMetrics messageMetrics, @Nullable final UserAgent userAgent) {
    this.messageMetrics = messageMetrics;
    this.userAgent = userAgent;
  }

  /// Store an envelope so it can be retrieved later via [#takeUnacknowledgedEnvelope].
  ///
  /// @param envelope An envelope that has been sent to a device and is waiting to be acknowledged
  void addUnacknowledgedEnvelope(final MessageProtos.Envelope envelope) {
    sentMessageCounter.incrementAndGet();
    final UUID messageGuid = UUIDUtil.fromByteString(envelope.getServerGuid());
    // If the envelope has a source, and it is not a server delivery receipt, it will be an ACI.
    final AciServiceIdentifier sourceId = envelope.hasSourceServiceId() && envelope.getType() != MessageProtos.Envelope.Type.SERVER_DELIVERY_RECEIPT
        ? AciServiceIdentifier.fromByteString(envelope.getSourceServiceId())
        : null;
    final ServiceIdentifier destinationId = ServiceIdentifier.fromByteString(envelope.getDestinationServiceId());
    final UnacknowledgedEnvelope unacknowledgedEnvelope = new UnacknowledgedEnvelope(messageGuid,
        envelope.getServerTimestamp(), envelope.getClientTimestamp(),
        envelope.getUrgent(), envelope.getEphemeral(),
        destinationId, sourceId);
    unacknowledgedEnvelopes.put(messageGuid, unacknowledgedEnvelope);
  }

  /// Take a previously stored envelope so it can be acknowledged. When the message is successfully removed from
  /// storage, call [UnacknowledgedEnvelope#handleMessageAcknowledged()].
  ///
  /// @param guid The server GUID of the envelope previously stored with [#addUnacknowledgedEnvelope(MessageProtos.Envelope)].
  /// @return The previously stored envelope, or null if no envelope with the provided guid was stored
  @Nullable
  UnacknowledgedEnvelope takeUnacknowledgedEnvelope(final UUID guid) {
    return unacknowledgedEnvelopes.remove(guid);
  }

  /// Signal that we've observed the end of the initial message queue. The Mono returned by [#queueDrained] will emit a
  /// value when all current envelopes tracked with [#addUnacknowledgedEnvelope] have been marked [UnacknowledgedEnvelope#handleMessageAcknowledged()]
  void markEndOfQueue() {
    messageMetrics
        .measureQueueDrain(MessageMetrics.GRPC_CHANNEL, userAgent, sentMessageCounter.get(), queueDrainStart);
    queueDrainEpoch.close();
  }

  /// Determine when a queue has been drained.
  ///
  /// @return A mono that completes with a QueueEmpty response when all messages tracked before [#markEndOfQueue] have
  /// been acknowledged.
  Mono<GetMessagesResponse> queueDrained() {
    return queueDrained.asMono();
  }


  class UnacknowledgedEnvelope {

    private final UUID serverGuid;
    private final long serverTimestamp;
    private final Timer.Sample sample;
    private final boolean inDrainEpoch;
    private final long clientTimestamp;
    private final boolean urgent;
    private final boolean ephemeral;
    private final ServiceIdentifier destinationServiceId;
    @Nullable private final AciServiceIdentifier sourceServiceId;

    private UnacknowledgedEnvelope(
        final UUID serverGuid,
        final long serverTimestamp,
        final long clientTimestamp,
        final boolean urgent,
        final boolean ephemeral,
        final ServiceIdentifier destinationServiceId,
        @Nullable final AciServiceIdentifier sourceServiceId) {
      this.serverGuid = serverGuid;
      this.serverTimestamp = serverTimestamp;
      this.clientTimestamp = clientTimestamp;
      this.urgent = urgent;
      this.ephemeral = ephemeral;
      this.destinationServiceId = destinationServiceId;
      this.sample = Timer.start();
      this.inDrainEpoch = queueDrainEpoch.tryArrive();
      this.sourceServiceId = sourceServiceId;
    }

    void handleMessageAcknowledged() {
      messageMetrics.measureSendMessageDuration(MessageMetrics.GRPC_CHANNEL, userAgent, sample);
      if (inDrainEpoch) {
        queueDrainEpoch.depart();
      }
    }

    public UUID getServerGuid() {
      return serverGuid;
    }

    public long getServerTimestamp() {
      return serverTimestamp;
    }

    public long getClientTimestamp() {
      return clientTimestamp;
    }

    public boolean isUrgent() {
      return urgent;
    }

    public boolean isEphemeral() {
      return ephemeral;
    }

    public ServiceIdentifier getDestinationServiceId() {
      return destinationServiceId;
    }

    /// Get the sender of this message
    ///
    /// @return the ServiceIdentifier of the source of this message. `null` if the message sender was not identified or
    /// if the envelope was for a server delivery receipt
    @Nullable
    public AciServiceIdentifier getSourceServiceId() {
      return sourceServiceId;
    }
  }
}
