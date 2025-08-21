/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;

class MessageMetricsTest {

  private final Account account = mock(Account.class);
  private final UUID aci = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private final UUID pni = UUID.fromString("22222222-2222-2222-2222-222222222222");
  private final UUID otherUuid = UUID.fromString("99999999-9999-9999-9999-999999999999");
  private MessageMetrics messageMetrics;
  private SimpleMeterRegistry simpleMeterRegistry;

  @BeforeEach
  void setup() {
    when(account.getUuid()).thenReturn(aci);
    when(account.getPhoneNumberIdentifier()).thenReturn(pni);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(aci))).thenReturn(true);
    when(account.isIdentifiedBy(new PniServiceIdentifier(pni))).thenReturn(true);
    simpleMeterRegistry = new SimpleMeterRegistry();
    messageMetrics = new MessageMetrics(simpleMeterRegistry);
  }

  @Test
  void measureAccountOutgoingMessageUuidMismatches() {

    final OutgoingMessageEntity outgoingMessageToAci = createOutgoingMessageEntity(new AciServiceIdentifier(aci));
    messageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageToAci);

    Optional<Counter> counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final OutgoingMessageEntity outgoingMessageToPni = createOutgoingMessageEntity(new PniServiceIdentifier(pni));
    messageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageToPni);
    counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final OutgoingMessageEntity outgoingMessageToOtherUuid = createOutgoingMessageEntity(new AciServiceIdentifier(otherUuid));
    messageMetrics.measureAccountOutgoingMessageUuidMismatches(account, outgoingMessageToOtherUuid);
    counter = findCounter(simpleMeterRegistry);

    assertEquals(1.0, counter.map(Counter::count).orElse(0.0));
  }

  private OutgoingMessageEntity createOutgoingMessageEntity(final ServiceIdentifier destinationIdentifier) {
    return new OutgoingMessageEntity(UUID.randomUUID(), 1, 1L, null, 1, destinationIdentifier, null, new byte[]{}, 1, true, false, null);
  }

  @Test
  void measureAccountEnvelopeUuidMismatches() {
    final MessageProtos.Envelope envelopeToAci = createEnvelope(new AciServiceIdentifier(aci));
    messageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToAci);

    Optional<Counter> counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final MessageProtos.Envelope envelopeToPni = createEnvelope(new PniServiceIdentifier(pni));
    messageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToPni);
    counter = findCounter(simpleMeterRegistry);

    assertTrue(counter.isEmpty());

    final MessageProtos.Envelope envelopeToOtherUuid = createEnvelope(new AciServiceIdentifier(otherUuid));
    messageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToOtherUuid);
    counter = findCounter(simpleMeterRegistry);

    assertEquals(1.0, counter.map(Counter::count).orElse(0.0));

    final MessageProtos.Envelope envelopeToNull = createEnvelope(null);
    messageMetrics.measureAccountEnvelopeUuidMismatches(account, envelopeToNull);
    counter = findCounter(simpleMeterRegistry);

    assertEquals(1.0, counter.map(Counter::count).orElse(0.0));
  }

  private MessageProtos.Envelope createEnvelope(ServiceIdentifier destinationIdentifier) {
    final MessageProtos.Envelope.Builder builder = MessageProtos.Envelope.newBuilder();

    if (destinationIdentifier != null) {
      builder.setDestinationServiceId(destinationIdentifier.toServiceIdentifierString());
    }

    return builder.build();
  }

  private Optional<Counter> findCounter(SimpleMeterRegistry meterRegistry) {
    final Optional<Meter> maybeMeter = meterRegistry.getMeters().stream()
        .filter(meter -> meter.getId().getName().contains(MessageMetrics.MISMATCHED_ACCOUNT_ENVELOPE_UUID_COUNTER_NAME))
        .findFirst();
    return maybeMeter.map(meter -> meter instanceof Counter ? (Counter) meter : null);
  }
}
