/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;

public final class MessageMetrics {

  private static final Logger logger = LoggerFactory.getLogger(MessageMetrics.class);

  static final String MISMATCHED_ACCOUNT_ENVELOPE_UUID_COUNTER_NAME = name(MessageMetrics.class,
      "mismatchedAccountEnvelopeUuid");

  public static final String DELIVERY_LATENCY_TIMER_NAME = name(MessageMetrics.class, "deliveryLatency");

  public static void measureAccountOutgoingMessageUuidMismatches(final Account account,
      final OutgoingMessageEntity outgoingMessage) {
    measureAccountDestinationUuidMismatches(account, outgoingMessage.destinationUuid());
  }

  public static void measureAccountEnvelopeUuidMismatches(final Account account,
      final MessageProtos.Envelope envelope) {
    if (envelope.hasDestinationUuid()) {
      try {
        measureAccountDestinationUuidMismatches(account, ServiceIdentifier.valueOf(envelope.getDestinationUuid()));
      } catch (final IllegalArgumentException ignored) {
        logger.warn("Envelope had invalid destination UUID: {}", envelope.getDestinationUuid());
      }
    }
  }

  private static void measureAccountDestinationUuidMismatches(final Account account, final ServiceIdentifier destinationIdentifier) {
    if (!account.isIdentifiedBy(destinationIdentifier)) {
      // In all cases, this represents a mismatch between the account’s current PNI and its PNI when the message was
      // sent. This is an expected case, but if this metric changes significantly, it could indicate an issue to
      // investigate.
      Metrics.counter(MISMATCHED_ACCOUNT_ENVELOPE_UUID_COUNTER_NAME).increment();
    }
  }

  public static void measureOutgoingMessageLatency(final long serverTimestamp,
      final String channel,
      final String userAgent,
      final ClientReleaseManager clientReleaseManager) {

    final List<Tag> tags = new ArrayList<>(3);
    tags.add(UserAgentTagUtil.getPlatformTag(userAgent));
    tags.add(Tag.of("channel", channel));

    UserAgentTagUtil.getClientVersionTag(userAgent, clientReleaseManager).ifPresent(tags::add);

    Timer.builder(DELIVERY_LATENCY_TIMER_NAME)
        .publishPercentileHistogram(true)
        .tags(tags)
        .register(Metrics.globalRegistry)
        .record(Duration.between(Instant.ofEpochMilli(serverTimestamp), Instant.now()));
  }
}
