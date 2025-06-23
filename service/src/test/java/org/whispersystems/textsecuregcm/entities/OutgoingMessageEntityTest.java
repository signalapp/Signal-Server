/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class OutgoingMessageEntityTest {

  @CartesianTest
  @CartesianTest.MethodFactory("roundTripThroughEnvelope")
  void roundTripThroughEnvelope(@Nullable final ServiceIdentifier sourceIdentifier,
      final ServiceIdentifier destinationIdentifier,
      @Nullable final UUID updatedPni) {

    final byte[] messageContent = TestRandomUtil.nextBytes(16);

    final long messageTimestamp = System.currentTimeMillis();
    final long serverTimestamp = messageTimestamp + 17;

    byte[] reportSpamToken = {1, 2, 3, 4, 5};

    final OutgoingMessageEntity outgoingMessageEntity = new OutgoingMessageEntity(
        UUID.randomUUID(),
        MessageProtos.Envelope.Type.CIPHERTEXT_VALUE,
        messageTimestamp,
        sourceIdentifier,
        sourceIdentifier != null ? (int) Device.PRIMARY_ID : 0,
        destinationIdentifier,
        updatedPni,
        messageContent,
        serverTimestamp,
        true,
        false,
        reportSpamToken);

    assertEquals(outgoingMessageEntity, OutgoingMessageEntity.fromEnvelope(outgoingMessageEntity.toEnvelope()));
  }

  @SuppressWarnings("unused")
  static ArgumentSets roundTripThroughEnvelope() {
    return ArgumentSets.argumentsForFirstParameter(new AciServiceIdentifier(UUID.randomUUID()),
            new PniServiceIdentifier(UUID.randomUUID()),
            null)
        .argumentsForNextParameter(new AciServiceIdentifier(UUID.randomUUID()),
            new PniServiceIdentifier(UUID.randomUUID()))
        .argumentsForNextParameter(UUID.randomUUID(), null);
  }

  @Test
  void entityPreservesEnvelope() {
    final byte[] reportSpamToken = TestRandomUtil.nextBytes(8);
    final AciServiceIdentifier sourceServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    final IncomingMessage message = new IncomingMessage(1, (byte) 44, 55, TestRandomUtil.nextBytes(4));

    MessageProtos.Envelope baseEnvelope = message.toEnvelope(
        new AciServiceIdentifier(UUID.randomUUID()),
        sourceServiceIdentifier,
        (byte) 123,
        System.currentTimeMillis(),
        false,
        false,
        true,
        reportSpamToken);

    MessageProtos.Envelope envelope = baseEnvelope.toBuilder().setServerGuid(UUID.randomUUID().toString()).build();

    // Note that outgoing message entities don't have an "ephemeral"/"online" flag
    assertEquals(envelope.toBuilder().clearEphemeral().build(),
        OutgoingMessageEntity.fromEnvelope(envelope).toEnvelope());
  }
}
