/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.identity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class AciServiceIdentifierTest {

  @Test
  void identityType() {
    assertEquals(IdentityType.ACI, new AciServiceIdentifier(UUID.randomUUID()).identityType());
  }

  @Test
  void toServiceIdentifierString() {
    final UUID uuid = UUID.randomUUID();

    assertEquals(uuid.toString(), new AciServiceIdentifier(uuid).toServiceIdentifierString());
  }

  @Test
  void toCompactByteArray() {
    final UUID uuid = UUID.randomUUID();

    assertArrayEquals(UUIDUtil.toBytes(uuid), new AciServiceIdentifier(uuid).toCompactByteArray());
  }

  @Test
  void toFixedWidthByteArray() {
    final UUID uuid = UUID.randomUUID();

    final ByteBuffer expectedBytesBuffer = ByteBuffer.allocate(17);
    expectedBytesBuffer.put((byte) 0x00);
    expectedBytesBuffer.putLong(uuid.getMostSignificantBits());
    expectedBytesBuffer.putLong(uuid.getLeastSignificantBits());
    expectedBytesBuffer.flip();

    assertArrayEquals(expectedBytesBuffer.array(), new AciServiceIdentifier(uuid).toFixedWidthByteArray());
  }

  @Test
  void valueOf() {
    final UUID uuid = UUID.randomUUID();

    assertEquals(uuid, AciServiceIdentifier.valueOf(uuid.toString()).uuid());
    assertThrows(IllegalArgumentException.class, () -> AciServiceIdentifier.valueOf("Not a valid UUID"));
    assertThrows(IllegalArgumentException.class, () -> AciServiceIdentifier.valueOf("PNI:" + uuid));
    assertThrows(IllegalArgumentException.class, () -> AciServiceIdentifier.valueOf("ACI:" + uuid).uuid());
  }


  @Test
  void fromBytes() {
    final UUID uuid = UUID.randomUUID();

    assertEquals(uuid, AciServiceIdentifier.fromBytes(UUIDUtil.toBytes(uuid)).uuid());

    final byte[] prefixedBytes = new byte[17];
    prefixedBytes[0] = 0x00;
    System.arraycopy(UUIDUtil.toBytes(uuid), 0, prefixedBytes, 1, 16);

    assertEquals(uuid, AciServiceIdentifier.fromBytes(prefixedBytes).uuid());

    prefixedBytes[0] = 0x01;

    assertThrows(IllegalArgumentException.class, () -> AciServiceIdentifier.fromBytes(prefixedBytes));
  }
}
