/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.protobuf.ByteString;
import com.webauthn4j.converter.jackson.deserializer.json.ByteArrayBase64Deserializer;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public record IncomingMessage(int type,
                              byte destinationDeviceId,
                              int destinationRegistrationId,

                              @JsonDeserialize(using = ByteArrayBase64Deserializer.class)
                              @NotNull
                              // Note that max size is validated elsewhere in the interest of controlling responses and
                              // reporting additional metrics.
                              @Size(min = 1)
                              byte[] content) {

  private static final String REJECT_INVALID_ENVELOPE_TYPE_COUNTER_NAME =
      MetricsUtil.name(IncomingMessage.class, "rejectInvalidEnvelopeType");

  public MessageProtos.Envelope toEnvelope(final ServiceIdentifier destinationIdentifier,
      @Nullable AciServiceIdentifier sourceServiceIdentifier,
      @Nullable Byte sourceDeviceId,
      final long timestamp,
      final boolean story,
      final boolean ephemeral,
      final boolean urgent,
      @Nullable byte[] reportSpamToken,
      final Clock clock) {

    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder();

    envelopeBuilder
        .setType(MessageProtos.Envelope.Type.forNumber(type))
        .setClientTimestamp(timestamp)
        .setServerTimestamp(clock.millis())
        .setDestinationServiceId(destinationIdentifier.toServiceIdentifierString())
        .setStory(story)
        .setEphemeral(ephemeral)
        .setUrgent(urgent);

    if (sourceServiceIdentifier != null && sourceDeviceId != null) {
      envelopeBuilder
          .setSourceServiceId(sourceServiceIdentifier.toServiceIdentifierString())
          .setSourceDevice(sourceDeviceId.intValue());
    }

    if (reportSpamToken != null) {
      envelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken));
    }

    envelopeBuilder.setContent(ByteString.copyFrom(content()));

    return envelopeBuilder.build();
  }

  @AssertTrue
  @Schema(hidden = true)
  public boolean isValidEnvelopeType() {
    if (type() == MessageProtos.Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE ||
        MessageProtos.Envelope.Type.forNumber(type()) == null) {

      Metrics.counter(REJECT_INVALID_ENVELOPE_TYPE_COUNTER_NAME).increment();

      return false;
    }

    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof IncomingMessage(int otherType, byte otherDeviceId, int otherRegistrationId, byte[] otherContent)))
      return false;
    return type == otherType && destinationDeviceId == otherDeviceId
        && destinationRegistrationId == otherRegistrationId && Objects.deepEquals(content, otherContent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, destinationDeviceId, destinationRegistrationId, Arrays.hashCode(content));
  }
}
