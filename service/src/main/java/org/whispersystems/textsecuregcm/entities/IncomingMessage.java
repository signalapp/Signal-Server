/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.google.protobuf.ByteString;
import java.util.Base64;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;

public record IncomingMessage(int type, byte destinationDeviceId, int destinationRegistrationId, String content) {

  public MessageProtos.Envelope toEnvelope(final ServiceIdentifier destinationIdentifier,
      @Nullable Account sourceAccount,
      @Nullable Byte sourceDeviceId,
      final long timestamp,
      final boolean story,
      final boolean urgent,
      @Nullable byte[] reportSpamToken) {

    final MessageProtos.Envelope.Type envelopeType = MessageProtos.Envelope.Type.forNumber(type());

    if (envelopeType == null) {
      throw new IllegalArgumentException("Bad envelope type: " + type());
    }

    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder();

    envelopeBuilder.setType(envelopeType)
        .setTimestamp(timestamp)
        .setServerTimestamp(System.currentTimeMillis())
        .setDestinationUuid(destinationIdentifier.toServiceIdentifierString())
        .setStory(story)
        .setUrgent(urgent);

    if (sourceAccount != null && sourceDeviceId != null) {
      envelopeBuilder
          .setSourceUuid(new AciServiceIdentifier(sourceAccount.getUuid()).toServiceIdentifierString())
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
}
