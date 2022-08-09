/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.storage.Account;

class MessageMetricsTest {

  private final Account account = mock(Account.class);
  private final UUID aci = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private final UUID pni = UUID.fromString("22222222-2222-2222-2222-222222222222");
  private final UUID otherUuid = UUID.fromString("99999999-9999-9999-9999-999999999999");
  private SimpleMeterRegistry simpleMeterRegistry;

  @BeforeEach
  void setup() {
    when(account.getUuid()).thenReturn(aci);
    when(account.getPhoneNumberIdentifier()).thenReturn(pni);
    Metrics.globalRegistry.clear();
    simpleMeterRegistry = new SimpleMeterRegistry();
    Metrics.globalRegistry.add(simpleMeterRegistry);
  }

  @AfterEach
  void teardown() {
    Metrics.globalRegistry.remove(simpleMeterRegistry);
    Metrics.globalRegistry.clear();
  }

  @Test
  void measureAccountOutgoingMessageUuidMismatches() {

    final OutgoingMessageEntity outgoingMessageToAci = createOutgoingMessageEntity(aci);
    MessageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageToAci);

    Optional<Counter> counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final OutgoingMessageEntity outgoingMessageToPni = createOutgoingMessageEntity(pni);
    MessageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageToPni);
    counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final OutgoingMessageEntity outgoingMessageToOtherUuid = createOutgoingMessageEntity(otherUuid);
    MessageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageToOtherUuid);
    counter = findCounter(simpleMeterRegistry);

    assertEquals(1.0, counter.map(Counter::count).orElse(0.0));
  }

  private OutgoingMessageEntity createOutgoingMessageEntity(UUID destinationUuid) {
    return new OutgoingMessageEntity(UUID.randomUUID(), 1, 1L, null, 1, destinationUuid, null, new byte[]{}, 1, true);
  }

  @Test
  void measureAccountEnvelopeUuidMismatches() {
    final MessageProtos.Envelope envelopeToAci = createEnvelope(aci);
    MessageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToAci);

    Optional<Counter> counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final MessageProtos.Envelope envelopeToPni = createEnvelope(pni);
    MessageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToPni);
    counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final MessageProtos.Envelope envelopeToOtherUuid = createEnvelope(otherUuid);
    MessageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToOtherUuid);
    counter = findCounter(simpleMeterRegistry);

    assertEquals(1.0, counter.map(Counter::count).orElse(0.0));

    final MessageProtos.Envelope envelopeToNull = createEnvelope(null);
    MessageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToNull);
    counter = findCounter(simpleMeterRegistry);

    assertEquals(1.0, counter.map(Counter::count).orElse(0.0));
  }

  private MessageProtos.Envelope createEnvelope(UUID destinationUuid) {
    final MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder();

    if (destinationUuid != null) {
      builder.setDestinationUuid(destinationUuid.toString());
    }

    return builder.build();
  }

  private Optional<Counter> findCounter(SimpleMeterRegistry meterRegistry) {
    final Optional<Meter> maybeMeter = meterRegistry.getMeters().stream().findFirst();
    return maybeMeter.map(meter -> meter instanceof Counter ? (Counter) meter : null);
  }
}
