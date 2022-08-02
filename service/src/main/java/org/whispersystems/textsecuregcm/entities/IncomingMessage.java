/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.storage.Account;

public record IncomingMessage(int type, long destinationDeviceId, int destinationRegistrationId, String content) {

  public MessageProtos.Envelope toEnvelope(final UUID destinationUuid,
      @Nullable Account sourceAccount,
      @Nullable Long sourceDeviceId,
      final long timestamp,
      final boolean urgent) {

    final MessageProtos.Envelope.Type envelopeType = MessageProtos.Envelope.Type.forNumber(type());

    if (envelopeType == null) {
      throw new IllegalArgumentException("Bad envelope type: " + type());
    }

    final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder();

    envelopeBuilder.setType(envelopeType)
        .setTimestamp(timestamp)
        .setServerTimestamp(System.currentTimeMillis())
        .setDestinationUuid(destinationUuid.toString())
        .setUrgent(urgent);

    if (sourceAccount != null && sourceDeviceId != null) {
      envelopeBuilder.setSourceUuid(sourceAccount.getUuid().toString())
          .setSourceDevice(sourceDeviceId.intValue());
    }

    if (StringUtils.isNotEmpty(content())) {
      envelopeBuilder.setContent(ByteString.copyFrom(Base64.getDecoder().decode(content())));
    }

    return envelopeBuilder.build();
  }
}
