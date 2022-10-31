/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.whispersystems.textsecuregcm.entities.MessageProtos;

public class MessageHelper {

  public static MessageProtos.Envelope createMessage(UUID senderUuid, final int senderDeviceId, UUID destinationUuid,
      long timestamp, String content) {
    return MessageProtos.Envelope.newBuilder()
        .setServerGuid(UUID.randomUUID().toString())
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setTimestamp(timestamp)
        .setServerTimestamp(0)
        .setSourceUuid(senderUuid.toString())
        .setSourceDevice(senderDeviceId)
        .setDestinationUuid(destinationUuid.toString())
        .setContent(ByteString.copyFrom(content.getBytes(StandardCharsets.UTF_8)))
        .build();
  }
}
