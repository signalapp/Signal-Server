/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.UUID;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.grpc.ServiceIdentifierUtil;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

/**
 * Provides utility methods for "compressing" and "expanding" envelopes. Historically UUID-like fields in envelopes have
 * been represented as strings (e.g. "c15f1dfb-ae2c-43a8-9bb9-baba97ac416c"), but <em>could</em> be represented as more
 * compact byte arrays instead. Existing clients generally expect string representations (though that should change in
 * the near future), but we can use the more compressed forms at rest for more efficient storage and transfer.
 */
public class EnvelopeUtil {

  @VisibleForTesting
  static final String INCLUDE_BINARY_SERVICE_ID_EXPERIMENT_NAME = "envelopeIncludeBinaryServiceIdentifier";

  /**
   * Converts all "compressible" UUID-like fields in the given envelope to more compact binary representations.
   *
   * @param envelope the envelope to compress
   *
   * @return an envelope with string-based UUID-like fields compressed to binary representations
   */
  public static MessageProtos.Envelope compress(final MessageProtos.Envelope envelope) {
    final MessageProtos.Envelope.Builder builder = envelope.toBuilder();
    
    if (builder.hasSourceServiceId()) {
      final ServiceIdentifier sourceServiceId = ServiceIdentifier.valueOf(builder.getSourceServiceId());
      
      builder.setSourceServiceIdBinary(ServiceIdentifierUtil.toCompactByteString(sourceServiceId));
      builder.clearSourceServiceId();
    }

    if (builder.hasDestinationServiceId()) {
      final ServiceIdentifier destinationServiceId = ServiceIdentifier.valueOf(builder.getDestinationServiceId());

      builder.setDestinationServiceIdBinary(ServiceIdentifierUtil.toCompactByteString(destinationServiceId));
      builder.clearDestinationServiceId();
    }

    if (builder.hasServerGuid()) {
      final UUID serverGuid = UUID.fromString(builder.getServerGuid());

      builder.setServerGuidBinary(UUIDUtil.toByteString(serverGuid));
      builder.clearServerGuid();
    }

    if (builder.hasUpdatedPni()) {
      final UUID updatedPni = UUID.fromString(builder.getUpdatedPni());

      builder.setUpdatedPniBinary(UUIDUtil.toByteString(updatedPni));
      builder.clearUpdatedPni();
    }

    return builder.build();
  }

  /**
   * "Expands" all binary representations of UUID-like fields to string representations to meet current client
   * expectations.
   *
   * @param envelope the envelope to expand
   *
   * @return an envelope with binary representations of UUID-like fields expanded to string representations
   */
  public static MessageProtos.Envelope expand(final MessageProtos.Envelope envelope,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    final boolean includeBinaryServiceIdentifiers;

    if (envelope.hasDestinationServiceIdBinary() || envelope.hasDestinationServiceId()) {
      final ServiceIdentifier destinationIdentifier = envelope.hasDestinationServiceIdBinary()
          ? ServiceIdentifier.fromBytes(envelope.getDestinationServiceIdBinary().toByteArray())
          : ServiceIdentifier.valueOf(envelope.getDestinationServiceId());

      includeBinaryServiceIdentifiers =
          experimentEnrollmentManager.isEnrolled(destinationIdentifier.uuid(), INCLUDE_BINARY_SERVICE_ID_EXPERIMENT_NAME);
    } else {
      includeBinaryServiceIdentifiers = false;
    }

    final MessageProtos.Envelope.Builder builder = envelope.toBuilder();

    if (builder.hasSourceServiceIdBinary()) {
      final ServiceIdentifier sourceServiceId =
          ServiceIdentifierUtil.fromByteString(builder.getSourceServiceIdBinary());

      builder.setSourceServiceId(sourceServiceId.toServiceIdentifierString());

      if (!includeBinaryServiceIdentifiers) {
        builder.clearSourceServiceIdBinary();
      }
    }

    if (builder.hasDestinationServiceIdBinary()) {
      final ServiceIdentifier destinationServiceId =
          ServiceIdentifierUtil.fromByteString(builder.getDestinationServiceIdBinary());

      builder.setDestinationServiceId(destinationServiceId.toServiceIdentifierString());

      if (!includeBinaryServiceIdentifiers) {
        builder.clearDestinationServiceIdBinary();
      }
    }

    if (builder.hasServerGuidBinary()) {
      final UUID serverGuid = UUIDUtil.fromByteString(builder.getServerGuidBinary());
      
      builder.setServerGuid(serverGuid.toString());

      if (!includeBinaryServiceIdentifiers) {
        builder.clearServerGuidBinary();
      }
    }

    if (builder.hasUpdatedPniBinary()) {
      final UUID updatedPni = UUIDUtil.fromByteString(builder.getUpdatedPniBinary());

      // Note that expanded envelopes include BOTH forms of the `updatedPni` field
      builder.setUpdatedPni(updatedPni.toString());
    }

    return builder.build();
  }
}
