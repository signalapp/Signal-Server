/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import jakarta.validation.constraints.AssertTrue;
import java.util.Base64;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;

public record IncomingMessage(int type, byte destinationDeviceId, int destinationRegistrationId, String content) {

  private static final String REJECT_INVALID_ENVELOPE_TYPE_COUNTER_NAME =
      MetricsUtil.name(IncomingMessage.class, "rejectInvalidEnvelopeType");

  public MessageProtos.Envelope toEnvelope(final ServiceIdentifier destinationIdentifier,
      @Nullable Account sourceAccount,
      @Nullable Byte sourceDeviceId,
      final long timestamp,
      final boolean story,
      final boolean ephemeral,
      final boolean urgent,
      @Nullable byte[] reportSpamToken) {

    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder();

    envelopeBuilder
        .setType(MessageProtos.Envelope.Type.forNumber(type))
        .setClientTimestamp(timestamp)
        .setServerTimestamp(System.currentTimeMillis())
        .setDestinationServiceId(destinationIdentifier.toServiceIdentifierString())
        .setStory(story)
        .setEphemeral(ephemeral)
        .setUrgent(urgent);

    if (sourceAccount != null && sourceDeviceId != null) {
      envelopeBuilder
          .setSourceServiceId(new AciServiceIdentifier(sourceAccount.getUuid()).toServiceIdentifierString())
          .setSourceDevice(sourceDeviceId.intValue());
    }

    if (reportSpamToken != null) {
      envelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken));
    }

    if (StringUtils.isNotEmpty(content())) {
      envelopeBuilder.setContent(ByteString.copyFrom(Base64.getDecoder().decode(content())));
    }

    return envelopeBuilder.build();
  }

  @AssertTrue
  public boolean isValidEnvelopeType() {
    if (type() == MessageProtos.Envelope.Type.SERVER_DELIVERY_RECEIPT_VALUE ||
        MessageProtos.Envelope.Type.forNumber(type()) == null) {

      Metrics.counter(REJECT_INVALID_ENVELOPE_TYPE_COUNTER_NAME).increment();

      return false;
    }

    return true;
  }
}
