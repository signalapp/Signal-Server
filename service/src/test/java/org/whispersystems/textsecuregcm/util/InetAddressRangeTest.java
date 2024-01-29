/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class InetAddressRangeTest {

  @ParameterizedTest
  @ValueSource(strings = {"192.168.0.1", "192.168.0.0/33", "$%#*(@!&^$/24", "192.168.0.0/fish", "signal.org"})
  void testBogusCidrBlock(final String cidrBlock) {
    assertThrows(IllegalArgumentException.class, () -> new InetAddressRange(cidrBlock));
  }

  @ParameterizedTest
  @MethodSource("argumentsForTestGeneratePrefixMask")
  void testGeneratePrefixMask(final int addressLengthBytes, final int prefixLengthBits, final byte[] expectedMask) {
    assertArrayEquals(expectedMask, InetAddressRange.generatePrefixMask(addressLengthBytes, prefixLengthBits));
  }

  private static Stream<Arguments> argumentsForTestGeneratePrefixMask() {
    return Stream.of(
        Arguments.of(4, 32, new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}),
        Arguments.of(4, 24, new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x00}),
        Arguments.of(4, 22, new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xfc, 0x00}),
        Arguments.of(4, 0, new byte[]{0x00, 0x00, 0x00, 0x00})
    );
  }

  @ParameterizedTest
  @MethodSource("argumentsForTestContains")
  void testContains(final String cidrBlock, final String address, final boolean expectContains) {
    assertEquals(expectContains, new InetAddressRange(cidrBlock).contains(address));
  }

  private static Stream<Arguments> argumentsForTestContains() {
    return Stream.of(
        Arguments.of("192.168.0.0/24", "192.168.0.1", true),
        Arguments.of("192.168.0.0/24", "192.168.1.0", false),
        Arguments.of("192.168.0.1/32", "192.168.0.1", true),
        Arguments.of("192.168.0.1/32", "192.168.0.0", false),
        Arguments.of("2001:db8::/48", "2001:db8:0:0:0:0:0:0", true),
        Arguments.of("2001:db8::/48", "2001:db8:0:ffff:ffff:ffff:ffff:ffff", true),
        Arguments.of("2001:db8::/48", "2001:db6:0:ffff:ffff:ffff:ffff:ffff", false)
    );
  }

  @Test
  void testContainsMismatchedAddressType() {
    assertFalse(new InetAddressRange("192.168.0.0/24").contains("2001:db8:0:0:0:0:0:0"));
    assertFalse(new InetAddressRange("2001:db8::/48").contains("192.168.0.1"));
  }

  @Test
  void testDeserialize() throws JsonProcessingException {
    final TypeReference<Map<String, InetAddressRange>> typeReference = new TypeReference<>() {};

    assertEquals(Map.of("range", new InetAddressRange("192.168.0.0/24")),
        new ObjectMapper().readValue("{\"range\":\"192.168.0.0/24\"}", typeReference));
  }
}
