/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.websocket.WebSocketConnection;

public final class MessageMetrics {

  private static final Logger logger = LoggerFactory.getLogger(MessageMetrics.class);

  public static String GRPC_CHANNEL = "grpc";
  public static String WEBSOCKET_CHANNEL = "websocket";

  @VisibleForTesting
  static final String MISMATCHED_ACCOUNT_ENVELOPE_UUID_COUNTER_NAME = name(MessageMetrics.class,
      "mismatchedAccountEnvelopeUuid");

  public static final String DELIVERY_LATENCY_TIMER_NAME = name(MessageMetrics.class, "deliveryLatency");

  // Uses [WebSocketConnection] for backwards compatibility
  private static final String SEND_MESSAGE_DURATION_TIMER_NAME = name(WebSocketConnection.class, "sendMessageDuration");
  private static final String INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME = name(WebSocketConnection.class, "initialQueueLength");
  private static final String INITIAL_QUEUE_DRAIN_TIMER_NAME = name(WebSocketConnection.class, "drainInitialQueue");
  private static final String SLOW_QUEUE_DRAIN_COUNTER_NAME = name(WebSocketConnection.class, "slowQueueDrain");
  private static final String DISPLACEMENT_COUNTER_NAME = name(WebSocketConnection.class, "displacement");
  private static final Duration SLOW_DRAIN_THRESHOLD = Duration.ofSeconds(10);

  private final MeterRegistry metricRegistry;
  private final Counter sendMessageCounter;
  private final Counter bytesSentCounter;

  @VisibleForTesting
  MessageMetrics(final MeterRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    sendMessageCounter = metricRegistry.counter(name(WebSocketConnection.class, "sendMessage"));
    bytesSentCounter = metricRegistry.counter(name(WebSocketConnection.class, "bytesSent"));
  }

  public MessageMetrics() {
    this(Metrics.globalRegistry);
  }

  public void measureAccountEnvelopeUuidMismatches(final Account account,
      final MessageProtos.Envelope envelope) {
    if (envelope.hasDestinationServiceId()) {
      try {
        measureAccountDestinationUuidMismatches(account, ServiceIdentifier.fromByteString(envelope.getDestinationServiceId()));
      } catch (final IllegalArgumentException ignored) {
        logger.warn("Envelope had invalid destination service ID: {}",
            HexFormat.of().formatHex(envelope.getDestinationServiceId().toByteArray()));
      }
    }
  }

  private void measureAccountDestinationUuidMismatches(final Account account, final ServiceIdentifier destinationIdentifier) {
    if (!account.isIdentifiedBy(destinationIdentifier)) {
      // In all cases, this represents a mismatch between the account’s current PNI and its PNI when the message was
      // sent. This is an expected case, but if this metric changes significantly, it could indicate an issue to
      // investigate.
      metricRegistry.counter(MISMATCHED_ACCOUNT_ENVELOPE_UUID_COUNTER_NAME).increment();
    }
  }

  public void measureOutgoingMessageLatency(final long serverTimestamp,
      final String channel,
      final boolean isPrimaryDevice,
      final boolean isUrgent,
      final boolean isEphemeral,
      @Nullable final UserAgent userAgent,
      final ClientReleaseManager clientReleaseManager) {

    final List<Tag> tags = new ArrayList<>();
    tags.add(UserAgentTagUtil.getPlatformTag(userAgent));
    tags.add(Tag.of("channel", channel));
    tags.add(Tag.of("isPrimary", String.valueOf(isPrimaryDevice)));
    tags.add(Tag.of("isUrgent", String.valueOf(isUrgent)));
    tags.add(Tag.of("isEphemeral", String.valueOf(isEphemeral)));

    UserAgentTagUtil.getClientVersionTag(userAgent, clientReleaseManager).ifPresent(tags::add);

    Timer.builder(DELIVERY_LATENCY_TIMER_NAME)
        .tags(tags)
        .register(metricRegistry)
        .record(Duration.between(Instant.ofEpochMilli(serverTimestamp), Instant.now()));
  }

  public void measureMessageStreamDisplaced(
      final String channel,
      @Nullable final UserAgent userAgent,
      final boolean connectedElsewhere) {
    metricRegistry.counter(
        DISPLACEMENT_COUNTER_NAME,
        Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of("connectedElsewhere", Boolean.toString(connectedElsewhere)),
            Tag.of("channel", channel)))
        .increment();
  }

  public void measureQueueDrain(
      final String channel,
      @Nullable final UserAgent userAgent,
      final long messagesDrained,
      final Timer.Sample drainStart) {
    final Tags tags = Tags.of(UserAgentTagUtil.getPlatformTag(userAgent), Tag.of("channel", channel));
    metricRegistry.summary(INITIAL_QUEUE_LENGTH_DISTRIBUTION_NAME, tags).record(messagesDrained);
    final long drainDurationNanos = drainStart.stop(metricRegistry.timer(INITIAL_QUEUE_DRAIN_TIMER_NAME, tags));
    if (Duration.ofNanos(drainDurationNanos).compareTo(SLOW_DRAIN_THRESHOLD) > 0) {
      metricRegistry.counter(SLOW_QUEUE_DRAIN_COUNTER_NAME, tags).increment();
    }
  }

  public void measureMessageSent(long messageByteLength) {
    sendMessageCounter.increment();
    bytesSentCounter.increment(messageByteLength);
  }

  public void measureSendMessageDuration(final String channel, @Nullable final UserAgent userAgent, final Timer.Sample sample) {
    sample.stop(Timer.builder(SEND_MESSAGE_DURATION_TIMER_NAME)
        .tags(Tags.of(UserAgentTagUtil.getPlatformTag(userAgent), Tag.of("channel", channel)))
        .register(metricRegistry));
  }
}
