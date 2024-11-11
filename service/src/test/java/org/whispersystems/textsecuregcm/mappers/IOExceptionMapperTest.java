/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IOExceptionMapperTest {

  @ParameterizedTest
  @MethodSource
  void testExceptionParsing(final IOException exception, final int expectedStatus) {

    try (Response response = new IOExceptionMapper().toResponse(exception)) {
      assertEquals(expectedStatus, response.getStatus());
    }
  }

  static Stream<Arguments> testExceptionParsing() {
    return Stream.of(
        Arguments.of(new IOException(), 503),
        Arguments.of(new IOException(new TimeoutException("A timeout")), 503),
        Arguments.of(new IOException(new TimeoutException()), 503),
        Arguments.of(new IOException(new TimeoutException("Idle timeout 30000 ms elapsed")), 408),
        Arguments.of(new IOException(new TimeoutException("Idle timeout expired")), 408),
        Arguments.of(new IOException(new RuntimeException(new TimeoutException("Idle timeout expired"))), 503),
        Arguments.of(new IOException(new TimeoutException("Idle timeout of another kind expired")), 503)
    );
  }

}
