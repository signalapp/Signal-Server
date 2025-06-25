/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.grpc.ServiceIdentifierUtil;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class EnvelopeUtilTest {

  @Test
  void compressExpand() {
    {
      final MessageProtos.Envelope compressibleFieldsNullMessage = generateRandomMessageBuilder().build();
      final MessageProtos.Envelope compressed = EnvelopeUtil.compress(compressibleFieldsNullMessage);

      assertFalse(compressed.hasSourceServiceId());
      assertFalse(compressed.hasSourceServiceIdBinary());
      assertFalse(compressed.hasDestinationServiceId());
      assertFalse(compressed.hasDestinationServiceIdBinary());
      assertFalse(compressed.hasServerGuid());
      assertFalse(compressed.hasServerGuidBinary());
      assertFalse(compressed.hasUpdatedPni());
      assertFalse(compressed.hasUpdatedPniBinary());

      final MessageProtos.Envelope expanded = EnvelopeUtil.expand(compressed);

      assertFalse(expanded.hasSourceServiceId());
      assertFalse(expanded.hasSourceServiceIdBinary());
      assertFalse(expanded.hasDestinationServiceId());
      assertFalse(expanded.hasDestinationServiceIdBinary());
      assertFalse(expanded.hasServerGuid());
      assertFalse(expanded.hasServerGuidBinary());
      assertFalse(compressed.hasUpdatedPni());
      assertFalse(compressed.hasUpdatedPniBinary());
    }

    {
      final ServiceIdentifier sourceServiceId = generateRandomServiceIdentifier();
      final ServiceIdentifier destinationServiceId = generateRandomServiceIdentifier();
      final UUID serverGuid = UUID.randomUUID();
      final UUID updatedPni = UUID.randomUUID();

      final MessageProtos.Envelope compressibleFieldsExpandedMessage = generateRandomMessageBuilder()
          .setSourceServiceId(sourceServiceId.toServiceIdentifierString())
          .setDestinationServiceId(destinationServiceId.toServiceIdentifierString())
          .setServerGuid(serverGuid.toString())
          .setUpdatedPni(updatedPni.toString())
          .build();

      final MessageProtos.Envelope compressed = EnvelopeUtil.compress(compressibleFieldsExpandedMessage);

      assertFalse(compressed.hasSourceServiceId());
      assertEquals(ServiceIdentifierUtil.toCompactByteString(sourceServiceId), compressed.getSourceServiceIdBinary());
      assertFalse(compressed.hasDestinationServiceId());
      assertEquals(ServiceIdentifierUtil.toCompactByteString(destinationServiceId), compressed.getDestinationServiceIdBinary());
      assertFalse(compressed.hasServerGuid());
      assertEquals(UUIDUtil.toByteString(serverGuid), compressed.getServerGuidBinary());
      assertFalse(compressed.hasUpdatedPni());
      assertEquals(UUIDUtil.toByteString(updatedPni), compressed.getUpdatedPniBinary());

      assertEquals(compressed, EnvelopeUtil.compress(compressed), "Double compression should make no changes");

      final MessageProtos.Envelope expanded = EnvelopeUtil.expand(compressed);

      assertEquals(sourceServiceId.toServiceIdentifierString(), expanded.getSourceServiceId());
      assertFalse(expanded.hasSourceServiceIdBinary());
      assertEquals(destinationServiceId.toServiceIdentifierString(), expanded.getDestinationServiceId());
      assertFalse(expanded.hasDestinationServiceIdBinary());
      assertEquals(serverGuid.toString(), expanded.getServerGuid());
      assertFalse(expanded.hasServerGuidBinary());
      assertEquals(updatedPni.toString(), expanded.getUpdatedPni());
      assertEquals(UUIDUtil.toByteString(updatedPni), expanded.getUpdatedPniBinary());

      assertEquals(expanded, EnvelopeUtil.expand(expanded), "Double expansion should make no changes");

      // Expanded envelopes include both representations of the `updatedPni` field
      assertEquals(compressibleFieldsExpandedMessage.toBuilder().setUpdatedPniBinary(UUIDUtil.toByteString(updatedPni)).build(),
          expanded);
    }
  }

  private static ServiceIdentifier generateRandomServiceIdentifier() {
    final IdentityType identityType = ThreadLocalRandom.current().nextBoolean() ? IdentityType.ACI : IdentityType.PNI;

    return switch (identityType) {
      case ACI -> new AciServiceIdentifier(UUID.randomUUID());
      case PNI -> new PniServiceIdentifier(UUID.randomUUID());
    };
  }

  private MessageProtos.Envelope.Builder generateRandomMessageBuilder() {
    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(ThreadLocalRandom.current().nextLong())
        .setServerTimestamp(ThreadLocalRandom.current().nextLong())
        .setContent(ByteString.copyFrom(TestRandomUtil.nextBytes(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT);
  }
}
