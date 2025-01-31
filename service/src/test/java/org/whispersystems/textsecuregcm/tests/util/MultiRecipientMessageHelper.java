/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import java.nio.ByteBuffer;
import java.util.List;

public class MultiRecipientMessageHelper {

  private MultiRecipientMessageHelper() {
  }

  public static byte[] generateMultiRecipientMessage(final List<TestRecipient> recipients) {
    return generateMultiRecipientMessage(recipients, 32);
  }

  public static byte[] generateMultiRecipientMessage(final List<TestRecipient> recipients, final int sharedPayloadSize) {
    if (sharedPayloadSize < 32) {
      throw new IllegalArgumentException("Shared payload size must be at least 32 bytes");
    }

    final ByteBuffer buffer = ByteBuffer.allocate(payloadSize(recipients, sharedPayloadSize));

    // first write the header
    buffer.put((byte) 0x23);  // version byte

    // count varint
    writeVarint(buffer, recipients.size());

    recipients.forEach(recipient -> {
      buffer.put(recipient.uuid().toFixedWidthByteArray());

      assert recipient.deviceIds().length == recipient.registrationIds().length;

      for (int i = 0; i < recipient.deviceIds().length; i++) {
        final int hasMore = i == recipient.deviceIds().length - 1 ? 0 : 0x8000;
        buffer.put(recipient.deviceIds()[i]); // device id (1 byte)
        buffer.putShort((short) (recipient.registrationIds()[i] | hasMore)); // registration id (2 bytes)
      }

      buffer.put(recipient.perRecipientKeyMaterial()); // key material (48 bytes)
    });

    // now write the actual message body (empty for now)
    writeVarint(buffer, sharedPayloadSize);
    buffer.put(new byte[sharedPayloadSize]);

    return buffer.array();
  }

  private static void writeVarint(final ByteBuffer buffer, long n) {
    if (n < 0) {
      throw new IllegalArgumentException();
    }

    while (n >= 0x80) {
      buffer.put ((byte) (n & 0x7F | 0x80));
      n >>= 7;
    }
    buffer.put((byte) (n & 0x7F));
  }

  private static int payloadSize(final List<TestRecipient> recipients, final int sharedPayloadSize) {
    final int fixedBytesPerRecipient = 17 // Service identifier length
        + 48; // Per-recipient key material

    final int bytesForDevices = 3 * recipients.stream()
        .mapToInt(recipient -> recipient.deviceIds().length)
        .sum();

    return 1 // Version byte
        + varintLength(recipients.size())
        + (recipients.size() * fixedBytesPerRecipient)
        + bytesForDevices
        + varintLength(sharedPayloadSize)
        + sharedPayloadSize;
  }

  private static int varintLength(long n) {
    int length = 0;

    while (n >= 0x80) {
      length += 1;
      n >>= 7;
    }

    return length + 1;
  }
}
