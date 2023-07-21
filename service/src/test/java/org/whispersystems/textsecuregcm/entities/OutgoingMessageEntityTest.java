/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import java.util.UUID;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

class OutgoingMessageEntityTest {

  @CartesianTest
  @CartesianTest.MethodFactory("roundTripThroughEnvelope")
  void roundTripThroughEnvelope(@Nullable final ServiceIdentifier sourceIdentifier,
      final ServiceIdentifier destinationIdentifier,
      @Nullable final UUID updatedPni) {

    final byte[] messageContent = new byte[16];
    new Random().nextBytes(messageContent);

    final long messageTimestamp = System.currentTimeMillis();
    final long serverTimestamp = messageTimestamp + 17;

    byte[] reportSpamToken = {1, 2, 3, 4, 5};

    final OutgoingMessageEntity outgoingMessageEntity = new OutgoingMessageEntity(
        UUID.randomUUID(),
        MessageProtos.Envelope.Type.CIPHERTEXT_VALUE,
        messageTimestamp,
        sourceIdentifier,
        sourceIdentifier != null ? (int) Device.MASTER_ID : 0,
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
    final Random random = new Random();

    final byte[] messageContent = new byte[16];
    random.nextBytes(messageContent);

    final byte[] reportSpamToken = new byte[8];
    random.nextBytes(reportSpamToken);

    final Account account = new Account();
    account.setUuid(UUID.randomUUID());

    IncomingMessage message = new IncomingMessage(1, 4444L, 55, "AAAAAA");

    MessageProtos.Envelope baseEnvelope = message.toEnvelope(
        new AciServiceIdentifier(UUID.randomUUID()),
        account,
        123L,
        System.currentTimeMillis(),
        false,
        true,
        reportSpamToken);

    MessageProtos.Envelope envelope = baseEnvelope.toBuilder().setServerGuid(UUID.randomUUID().toString()).build();

    assertEquals(envelope, OutgoingMessageEntity.fromEnvelope(envelope).toEnvelope());
  }
}
