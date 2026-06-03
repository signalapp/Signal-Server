/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.grpc.ServiceIdentifierUtil;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class EnvelopeUtilTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void compressExpand(final boolean includeBinaryServiceIdentifiers) {
    final ExperimentEnrollmentManager experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);
    when(experimentEnrollmentManager.isEnrolled(any(UUID.class), eq(EnvelopeUtil.INCLUDE_BINARY_SERVICE_ID_EXPERIMENT_NAME)))
        .thenReturn(includeBinaryServiceIdentifiers);

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

      final MessageProtos.Envelope expanded = EnvelopeUtil.expand(compressed, experimentEnrollmentManager);

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

      final MessageProtos.Envelope expanded = EnvelopeUtil.expand(compressed, experimentEnrollmentManager);

      assertEquals(sourceServiceId.toServiceIdentifierString(), expanded.getSourceServiceId());
      assertEquals(destinationServiceId.toServiceIdentifierString(), expanded.getDestinationServiceId());
      assertEquals(serverGuid.toString(), expanded.getServerGuid());
      assertEquals(updatedPni.toString(), expanded.getUpdatedPni());
      assertEquals(UUIDUtil.toByteString(updatedPni), expanded.getUpdatedPniBinary());

      if (includeBinaryServiceIdentifiers) {
        assertEquals(ServiceIdentifierUtil.toCompactByteString(sourceServiceId), expanded.getSourceServiceIdBinary());
        assertEquals(ServiceIdentifierUtil.toCompactByteString(destinationServiceId), expanded.getDestinationServiceIdBinary());
        assertEquals(UUIDUtil.toByteString(serverGuid), expanded.getServerGuidBinary());
      } else {
        assertFalse(expanded.hasSourceServiceIdBinary());
        assertFalse(expanded.hasDestinationServiceIdBinary());
        assertFalse(expanded.hasServerGuidBinary());
      }

      assertEquals(expanded, EnvelopeUtil.expand(expanded, experimentEnrollmentManager),
          "Double expansion should make no changes");

      // Expanded envelopes include both representations of the `updatedPni` field
      assertTrue(expanded.hasUpdatedPni());
      assertTrue(expanded.hasUpdatedPniBinary());
      assertEquals(UUID.fromString(expanded.getUpdatedPni()), UUIDUtil.fromByteString(expanded.getUpdatedPniBinary()));
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
