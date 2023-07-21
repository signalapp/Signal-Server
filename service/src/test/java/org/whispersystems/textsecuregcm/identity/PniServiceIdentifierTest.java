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

class PniServiceIdentifierTest {

  @Test
  void identityType() {
    assertEquals(IdentityType.PNI, new PniServiceIdentifier(UUID.randomUUID()).identityType());
  }

  @Test
  void toServiceIdentifierString() {
    final UUID uuid = UUID.randomUUID();

    assertEquals("PNI:" + uuid, new PniServiceIdentifier(uuid).toServiceIdentifierString());
  }

  @Test
  void toByteArray() {
    final UUID uuid = UUID.randomUUID();

    final ByteBuffer expectedBytesBuffer = ByteBuffer.allocate(17);
    expectedBytesBuffer.put((byte) 0x01);
    expectedBytesBuffer.putLong(uuid.getMostSignificantBits());
    expectedBytesBuffer.putLong(uuid.getLeastSignificantBits());
    expectedBytesBuffer.flip();

    assertArrayEquals(expectedBytesBuffer.array(), new PniServiceIdentifier(uuid).toCompactByteArray());
    assertArrayEquals(expectedBytesBuffer.array(), new PniServiceIdentifier(uuid).toFixedWidthByteArray());
  }

  @Test
  void valueOf() {
    final UUID uuid = UUID.randomUUID();

    assertEquals(uuid, PniServiceIdentifier.valueOf("PNI:" + uuid).uuid());
    assertThrows(IllegalArgumentException.class, () -> PniServiceIdentifier.valueOf(uuid.toString()));
    assertThrows(IllegalArgumentException.class, () -> PniServiceIdentifier.valueOf("Not a valid UUID"));
    assertThrows(IllegalArgumentException.class, () -> PniServiceIdentifier.valueOf("ACI:" + uuid));
  }

  @Test
  void fromBytes() {
    final UUID uuid = UUID.randomUUID();

    assertThrows(IllegalArgumentException.class, () -> PniServiceIdentifier.fromBytes(UUIDUtil.toBytes(uuid)));

    final byte[] prefixedBytes = new byte[17];
    prefixedBytes[0] = 0x00;
    System.arraycopy(UUIDUtil.toBytes(uuid), 0, prefixedBytes, 1, 16);

    assertThrows(IllegalArgumentException.class, () -> PniServiceIdentifier.fromBytes(prefixedBytes));

    prefixedBytes[0] = 0x01;

    assertEquals(uuid, PniServiceIdentifier.fromBytes(prefixedBytes).uuid());
  }
}
