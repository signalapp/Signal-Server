/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class AttributeValuesTest {
  @Test
  void testUUIDRoundTrip() {
    UUID orig = UUID.randomUUID();
    AttributeValue av = AttributeValues.fromUUID(orig);
    UUID returned = AttributeValues.getUUID(Map.of("foo", av), "foo", null);
    assertEquals(orig, returned);
  }

  @Test
  void testLongRoundTrip() {
    long orig = 12345;
    AttributeValue av = AttributeValues.fromLong(orig);
    long returned = AttributeValues.getLong(Map.of("foo", av), "foo", -1);
    assertEquals(orig, returned);
  }

  @Test
  void testIntRoundTrip() {
    int orig = 12345;
    AttributeValue av = AttributeValues.fromInt(orig);
    int returned = AttributeValues.getInt(Map.of("foo", av), "foo", -1);
    assertEquals(orig, returned);
  }

  @Test
  void testByteBuffer() {
    byte[] bytes = {1, 2, 3};
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    AttributeValue av = AttributeValues.fromByteBuffer(bb);
    byte[] returned = av.b().asByteArray();
    assertArrayEquals(bytes, returned);
    returned = AttributeValues.getByteArray(Map.of("foo", av), "foo", null);
    assertArrayEquals(bytes, returned);
  }

  @Test
  void testByteBuffer2() {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[8]);
    byteBuffer.putLong(123);
    assertEquals(byteBuffer.remaining(), 0);
    AttributeValue av = AttributeValues.fromByteBuffer(byteBuffer.flip());
    assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}, AttributeValues.getByteArray(Map.of("foo", av), "foo", null));
  }
}
