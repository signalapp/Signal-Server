/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HeaderUtilsTest {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @ParameterizedTest
    @MethodSource("argumentsForGetMostRecentProxy")
    void getMostRecentProxy(final String forwardedFor, final Optional<String> expectedMostRecentProxy) {
      assertEquals(expectedMostRecentProxy, HeaderUtils.getMostRecentProxy(forwardedFor));
    }

    private static Stream<Arguments> argumentsForGetMostRecentProxy() {
      return Stream.of(
          arguments(null, Optional.empty()),
          arguments("", Optional.empty()),
          arguments("    ", Optional.empty()),
          arguments("203.0.113.195,", Optional.empty()),
          arguments("203.0.113.195, ", Optional.empty()),
          arguments("203.0.113.195", Optional.of("203.0.113.195")),
          arguments("203.0.113.195, 70.41.3.18, 150.172.238.178", Optional.of("150.172.238.178"))
      );
    }
}
