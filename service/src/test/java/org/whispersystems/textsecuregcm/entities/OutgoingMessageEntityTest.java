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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Device;

class OutgoingMessageEntityTest {

  @ParameterizedTest
  @MethodSource
  void toFromEnvelope(@Nullable final String source, @Nullable final UUID sourceUuid, @Nullable final UUID updatedPni) {
    final byte[] messageContent = new byte[16];
    new Random().nextBytes(messageContent);

    final long messageTimestamp = System.currentTimeMillis();
    final long serverTimestamp = messageTimestamp + 17;

    final OutgoingMessageEntity outgoingMessageEntity = new OutgoingMessageEntity(UUID.randomUUID(),
        MessageProtos.Envelope.Type.CIPHERTEXT_VALUE,
        messageTimestamp,
        "+18005551234",
        UUID.randomUUID(),
        source != null ? (int) Device.MASTER_ID : 0,
        UUID.randomUUID(),
        UUID.randomUUID(),
        messageContent,
        serverTimestamp);

    assertEquals(outgoingMessageEntity, OutgoingMessageEntity.fromEnvelope(outgoingMessageEntity.toEnvelope()));
  }

  private static Stream<Arguments> toFromEnvelope() {
    return Stream.of(
        Arguments.of("+18005551234", UUID.randomUUID(), UUID.randomUUID()),
        Arguments.of("+18005551234", UUID.randomUUID(), null),
        Arguments.of(null, null, UUID.randomUUID()));
  }
}
