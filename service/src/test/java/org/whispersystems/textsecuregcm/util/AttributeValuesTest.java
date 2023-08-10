/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

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

  @Test
  void testNullUuid() {
    final Map<String, AttributeValue> item = Map.of("key", AttributeValue.builder().nul(true).build());
    assertNull(AttributeValues.getUUID(item, "key", null));
  }

  @ParameterizedTest
  @MethodSource
  void extractByteArray(final AttributeValue attributeValue, final byte[] expectedByteArray) {
    assertArrayEquals(expectedByteArray, AttributeValues.extractByteArray(attributeValue, "counter"));
  }

  private static Stream<Arguments> extractByteArray() {
    final byte[] key = Base64.getDecoder().decode("c+k+8zv8WaFdDjR9IOvCk6BcY5OI7rge/YUDkaDGyRc=");

    return Stream.of(
        Arguments.of(AttributeValue.fromB(SdkBytes.fromByteArray(key)), key),
        Arguments.of(AttributeValue.fromS(Base64.getEncoder().encodeToString(key)), key),
        Arguments.of(AttributeValue.fromS(Base64.getEncoder().withoutPadding().encodeToString(key)), key)
    );
  }

  @ParameterizedTest
  @MethodSource
  void extractByteArrayIllegalArgument(final AttributeValue attributeValue) {
    assertThrows(IllegalArgumentException.class, () -> AttributeValues.extractByteArray(attributeValue, "counter"));
  }

  private static Stream<Arguments> extractByteArrayIllegalArgument() {
    return Stream.of(
        Arguments.of(AttributeValue.fromN("12")),
        Arguments.of(AttributeValue.fromS("")),
        Arguments.of(AttributeValue.fromS("Definitely not legitimate base64 👎"))
    );
  }
}
