/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

public record RemovedMessage(Optional<ServiceIdentifier> sourceServiceId, ServiceIdentifier destinationServiceId,
                             @VisibleForTesting UUID serverGuid, long serverTimestamp, long clientTimestamp,
                             MessageProtos.Envelope.Type envelopeType) {

  public static RemovedMessage fromEnvelope(MessageProtos.Envelope envelope) {
    return new RemovedMessage(
        envelope.hasSourceServiceId()
            ? Optional.of(ServiceIdentifier.valueOf(envelope.getSourceServiceId()))
            : Optional.empty(),
        ServiceIdentifier.valueOf(envelope.getDestinationServiceId()),
        UUID.fromString(envelope.getServerGuid()),
        envelope.getServerTimestamp(),
        envelope.getClientTimestamp(),
        envelope.getType()
    );
  }
}
