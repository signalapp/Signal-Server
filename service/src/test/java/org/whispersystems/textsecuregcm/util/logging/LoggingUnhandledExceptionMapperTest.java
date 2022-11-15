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
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.security.Principal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class LoggingUnhandledExceptionMapperTest {

  private static final Logger logger = mock(Logger.class);

  private static final LoggingUnhandledExceptionMapper exceptionMapper = spy(new LoggingUnhandledExceptionMapper(logger));

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(exceptionMapper)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new TestController())
      .build();

  static Stream<Arguments> testExceptionMapper() {
    return Stream.of(
        Arguments.of(false, "/v1/test/no-exception", "/v1/test/no-exception", "Signal-Android/5.1.2 Android/30", null),
        Arguments.of(true, "/v1/test/unhandled-runtime-exception", "/v1/test/unhandled-runtime-exception", "Signal-Android/5.1.2 Android/30", "ANDROID 5.1.2"),
        Arguments.of(true, "/v1/test/unhandled-runtime-exception/1/and/two", "/v1/test/unhandled-runtime-exception/\\{parameter1\\}/and/\\{parameter2\\}", "Signal-iOS/5.10.2 iOS/14.1", "IOS 5.10.2"),
        Arguments.of(true, "/v1/test/unhandled-runtime-exception", "/v1/test/unhandled-runtime-exception", "Some literal user-agent", "Some literal user-agent")
    );
  }

  @AfterEach
  void teardown() {
    reset(exceptionMapper, logger);
  }

  @ParameterizedTest
  @MethodSource
  void testExceptionMapper(final boolean expectException, final String targetPath, final String loggedPath,
      final String userAgentHeader, final String userAgentLog) {

    resources.getJerseyTest()
        .target(targetPath)
        .request()
        .header(HttpHeaders.USER_AGENT, userAgentHeader)
        .get();

    if (expectException) {
      verify(exceptionMapper, times(1)).toResponse(any(Exception.class));
      verify(logger, times(1))
          .error(matches(String.format(".* at GET %s \\(%s\\)", loggedPath, userAgentLog)), any(Exception.class));

    } else {
      verifyNoInteractions(exceptionMapper);
    }
  }

  @ParameterizedTest
  @MethodSource("testExceptionMapper")
  void testWebsocketExceptionMapper(final boolean expectException, final String targetPath, final String loggedPath,
      final String userAgentHeader, final String userAgentLog) {

    Session session = mock(Session.class);
    WebSocketResourceProvider<TestPrincipal> provider = createWebsocketProvider(userAgentHeader, session);

    provider.onWebSocketConnect(session);

    byte[] message = new ProtobufWebSocketMessageFactory()
        .createRequest(Optional.of(111L), "GET", targetPath, new LinkedList<>(), Optional.empty()).toByteArray();

    provider.onWebSocketBinary(message, 0, message.length);

    if (expectException) {
      verify(exceptionMapper, times(1)).toResponse(any(Exception.class));
      verify(logger, times(1))
          .error(matches(String.format(".* at GET %s \\(%s\\)", loggedPath, userAgentLog)), any(Exception.class));

    } else {
      verifyNoInteractions(exceptionMapper);
    }

  }

  private WebSocketResourceProvider<TestPrincipal> createWebsocketProvider(final String userAgentHeader, final Session session) {
    ResourceConfig resourceConfig = new DropwizardResourceConfig();
    resourceConfig.register(exceptionMapper);
    resourceConfig.register(new TestController());
    resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
    resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
    resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

    ApplicationHandler applicationHandler = new ApplicationHandler(resourceConfig);
    WebsocketRequestLog requestLog = mock(WebsocketRequestLog.class);
    WebSocketResourceProvider<TestPrincipal> provider = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler,
        requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

    RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
    UpgradeRequest request = mock(UpgradeRequest.class);

    when(session.getUpgradeRequest()).thenReturn(request);
    when(session.getRemote()).thenReturn(remoteEndpoint);
    when(request.getHeader(HttpHeaders.USER_AGENT)).thenReturn(userAgentHeader);
    when(request.getHeaders()).thenReturn(Map.of(HttpHeaders.USER_AGENT, List.of(userAgentHeader)));

    return provider;
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
    public Response testUnhandledExceptionWithPathParameter(@PathParam("parameter1") String parameter1,
        @PathParam("parameter2") String parameter2) {
      throw new RuntimeException();
    }
  }

  public static class TestPrincipal implements Principal {

    private final String name;

    private TestPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
