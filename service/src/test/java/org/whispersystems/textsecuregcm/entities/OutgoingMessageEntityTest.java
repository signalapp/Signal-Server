/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

class OutgoingMessageEntityTest {

  @ParameterizedTest
  @MethodSource
  void roundTripThroughEnvelope(@Nullable final UUID sourceUuid, @Nullable final UUID updatedPni) {
    final byte[] messageContent = new byte[16];
    new Random().nextBytes(messageContent);

    final long messageTimestamp = System.currentTimeMillis();
    final long serverTimestamp = messageTimestamp + 17;

    byte[] reportSpamToken = {1, 2, 3, 4, 5};

    final OutgoingMessageEntity outgoingMessageEntity = new OutgoingMessageEntity(
        UUID.randomUUID(),
        MessageProtos.Envelope.Type.CIPHERTEXT_VALUE,
        messageTimestamp,
        UUID.randomUUID(),
        sourceUuid != null ? (int) Device.MASTER_ID : 0,
        UUID.randomUUID(),
        updatedPni,
        messageContent,
        serverTimestamp,
        true,
        false,
        reportSpamToken);

    assertEquals(outgoingMessageEntity, OutgoingMessageEntity.fromEnvelope(outgoingMessageEntity.toEnvelope()));
  }

  private static Stream<Arguments> roundTripThroughEnvelope() {
    return Stream.of(
        Arguments.of(UUID.randomUUID(), UUID.randomUUID()),
        Arguments.of(UUID.randomUUID(), null),
        Arguments.of(null, UUID.randomUUID()));
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
        UUID.randomUUID(),
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
