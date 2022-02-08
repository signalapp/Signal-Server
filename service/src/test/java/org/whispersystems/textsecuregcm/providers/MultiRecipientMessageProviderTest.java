/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.stream.Stream;

import javax.ws.rs.WebApplicationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MultiRecipientMessageProviderTest {

  static byte[] createByteArray(int... bytes) {
    byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      result[i] = (byte)bytes[i];
    }
    return result;
  }

  static Stream<Arguments> readU16TestCases() {
    return Stream.of(
        arguments(0xFFFE, createByteArray(0xFF, 0xFE)),
        arguments(0x0001, createByteArray(0x00, 0x01)),
        arguments(0xBEEF, createByteArray(0xBE, 0xEF)),
        arguments(0xFFFF, createByteArray(0xFF, 0xFF)),
        arguments(0x0000, createByteArray(0x00, 0x00)),
        arguments(0xF080, createByteArray(0xF0, 0x80))
    );
  }

  @ParameterizedTest
  @MethodSource("readU16TestCases")
  void testReadU16(int expectedValue, byte[] input) throws Exception {
    try (final ByteArrayInputStream stream = new ByteArrayInputStream(input)) {
      assertThat(MultiRecipientMessageProvider.readU16(stream)).isEqualTo(expectedValue);
    }
  }

  static Stream<Arguments> readVarintTestCases() {
    return Stream.of(
        arguments(0x00L, createByteArray(0x00)),
        arguments(0x01L, createByteArray(0x01)),
        arguments(0x7FL, createByteArray(0x7F)),
        arguments(0x80L, createByteArray(0x80, 0x01)),
        arguments(0xFFL, createByteArray(0xFF, 0x01)),
        arguments(0b1010101_0011001_1100110L, createByteArray(0b1_1100110, 0b1_0011001, 0b0_1010101)),
        arguments(0x7FFFFFFF_FFFFFFFFL, createByteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F)),
        arguments(-1L, createByteArray(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01))
    );
  }

  @ParameterizedTest
  @MethodSource("readVarintTestCases")
  void testReadVarint(long expectedValue, byte[] input) throws Exception {
    try (final ByteArrayInputStream stream = new ByteArrayInputStream(input)) {
      assertThat(MultiRecipientMessageProvider.readVarint(stream)).isEqualTo(expectedValue);
    }
  }

  @Test
  void testVarintEOF() {
    assertThatThrownBy(() -> MultiRecipientMessageProvider.readVarint(new ByteArrayInputStream(createByteArray(0xFF, 0x80))))
      .isInstanceOf(IOException.class);
    assertThatThrownBy(() -> MultiRecipientMessageProvider.readVarint(new ByteArrayInputStream(createByteArray())))
      .isInstanceOf(IOException.class);
  }

  @Test
  void testVarintTooBig() {
    assertThatThrownBy(() -> MultiRecipientMessageProvider.readVarint(new ByteArrayInputStream(createByteArray(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02))))
      .isInstanceOf(WebApplicationException.class);
    assertThatThrownBy(() -> MultiRecipientMessageProvider.readVarint(new ByteArrayInputStream(createByteArray(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80))))
      .isInstanceOf(WebApplicationException.class);
  }

}
