/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.logging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.util.stream.Stream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;

@ExtendWith(DropwizardExtensionsSupport.class)
class LoggingUnhandledExceptionMapperTest {

  private static final Logger logger = mock(Logger.class);

  private static LoggingUnhandledExceptionMapper exceptionMapper = spy(new LoggingUnhandledExceptionMapper(logger));

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(exceptionMapper)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new TestController())
      .build();

  @BeforeEach
  void setup() {
    reset(exceptionMapper);
  }

  @ParameterizedTest
  @MethodSource
  void testExceptionMapper(final boolean expectException, final String targetPath, final String loggedPath, final String userAgentHeader,
      final String userAgentLog) {

    resources.getJerseyTest()
        .target(targetPath)
        .request()
        .header("User-Agent", userAgentHeader)
        .get();

    if (expectException) {

      verify(exceptionMapper, times(1)).toResponse(any(Exception.class));
      verify(logger, times(1)).error(matches(String.format(".* at GET %s \\(%s\\)", loggedPath, userAgentLog)), any(Exception.class));

    } else {
      verifyNoInteractions(exceptionMapper);
    }
  }

  static Stream<Arguments> testExceptionMapper() {
    return Stream.of(
        Arguments.of(false, "/v1/test/no-exception", "/v1/test/no-exception", null, null, null),
        Arguments.of(true, "/v1/test/unhandled-runtime-exception", "/v1/test/unhandled-runtime-exception", "Signal-Android/5.1.2 Android/30", "ANDROID 5.1.2"),
        Arguments.of(true, "/v1/test/unhandled-runtime-exception/1/and/two", "/v1/test/unhandled-runtime-exception/\\{parameter1\\}/and/\\{parameter2\\}", "Signal-iOS/5.10.2 iOS/14.1", "IOS 5.10.2")
    );
  }

  @Path("/v1/test")
  public static class TestController {

    @GET
    @Path("/no-exception")
    public Response testNoException() {
      return Response.ok().build();
    }

    @GET
    @Path("/unhandled-runtime-exception")
    public Response testUnhandledException() {
      throw new RuntimeException();
    }

    @GET
    @Path("/unhandled-runtime-exception/{parameter1}/and/{parameter2}")
    public Response testUnhandledExceptionWithPathParameter(@PathParam("parameter1") String parameter1, @PathParam("parameter2") String parameter2) {
      throw new RuntimeException();
    }
  }
}
