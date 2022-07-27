/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayInputStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MultiRecipientMessageProviderTest {

  static byte[] createTwoByteArray(int b1, int b2) {
    return new byte[]{(byte) b1, (byte) b2};
  }

  static Stream<Arguments> readU16TestCases() {
    return Stream.of(
        arguments(0xFFFE, createTwoByteArray(0xFF, 0xFE)),
        arguments(0x0001, createTwoByteArray(0x00, 0x01)),
        arguments(0xBEEF, createTwoByteArray(0xBE, 0xEF)),
        arguments(0xFFFF, createTwoByteArray(0xFF, 0xFF)),
        arguments(0x0000, createTwoByteArray(0x00, 0x00)),
        arguments(0xF080, createTwoByteArray(0xF0, 0x80))
    );
  }

  @ParameterizedTest
  @MethodSource("readU16TestCases")
  void testReadU16(int expectedValue, byte[] input) throws Exception {
    try (final ByteArrayInputStream stream = new ByteArrayInputStream(input)) {
      assertThat(MultiRecipientMessageProvider.readU16(stream)).isEqualTo(expectedValue);
    }
  }
}
