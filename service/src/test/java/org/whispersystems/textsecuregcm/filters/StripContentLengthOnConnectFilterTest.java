/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.ServletContext;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.FilterMapping;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.ee10.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.http.HostPortHttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.Jetty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
class StripContentLengthOnConnectFilterTest {

  private static final String WEBSOCKET_WITH_FILTER_PATH = "/websocket/filtered";
  private static final String WEBSOCKET_WITHOUT_FILTER_PATH = "/websocket/unfiltered";

  private static final DropwizardAppExtension<Configuration> EXTENSION =
      new DropwizardAppExtension<>(TestApplicationWithFilter.class, null,
          ConfigOverride.config("server.applicationConnectors[0].type", "h2c"),
          ConfigOverride.config("server.applicationConnectors[0].port", "0"));


  @Test
  void contentLengthIsStrippedOnUpgrade() throws Exception {
    final HttpFields fields = sendUpgradeRequest(WEBSOCKET_WITH_FILTER_PATH);
    assertNull(fields.getField("content-length"));
  }

  @Test
  void contentLengthIncorrectlyIncludedOnUpgrade() throws Exception {
    final HttpFields fields = sendUpgradeRequest(WEBSOCKET_WITHOUT_FILTER_PATH);
    assertNotNull(fields.getField("content-length"), """
        If this fails, our jetty version no longer includes errant content-lengths on H2 connect responses.
        StripContentLengthOnConnectFilter can now be removed.
        """);
  }

  @Test
  void versionCheck() {
    assertEquals("12.1.5", Jetty.VERSION, "This class can be removed with https://github.com/jetty/jetty.project/issues/15074, likely 12.1.10");
  }

  private static HttpFields sendUpgradeRequest(final String path) throws Exception {
    final int port = EXTENSION.getLocalPort();
    try (final HTTP2Client client = new HTTP2Client()) {
      client.start();
      final Session session =
          client.connect(new InetSocketAddress("localhost", port), new Session.Listener() {}).join();
      final HttpFields requestFields = HttpFields.build().put("sec-websocket-version", "13");
      final HostPortHttpField hostPort = new HostPortHttpField("localhost:" + port);
      final MetaData.ConnectRequest connect =
          new MetaData.ConnectRequest(HttpScheme.HTTP, hostPort, path, requestFields, "websocket");
      final HeadersFrame headersFrame = new HeadersFrame(connect, null, false);

      final CompletableFuture<MetaData.Response> responseFuture = new CompletableFuture<>();
      Stream.Listener streamListener = new Stream.Listener() {
        @Override
        public void onHeaders(Stream stream, HeadersFrame frame) {
          if (frame.getMetaData().isResponse()) {
            responseFuture.complete((MetaData.Response) frame.getMetaData());
          }
        }
      };
      session.newStream(headersFrame, streamListener).get(5, TimeUnit.SECONDS);
      final MetaData.Response response = responseFuture.get(5, TimeUnit.SECONDS);
      return response.getHttpFields();
    }
  }

  private static class NoopWebSocket implements org.eclipse.jetty.websocket.api.Session.Listener.AutoDemanding {}

  public static class TestApplicationWithFilter extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration, final Environment environment) throws Exception {
      final JettyWebSocketCreator jwsc = (_, _) -> new NoopWebSocket();
      JettyWebSocketServletContainerInitializer.configure(
          environment.getApplicationContext(),
          (servletContext, container) -> {

            container.addMapping(WEBSOCKET_WITH_FILTER_PATH, jwsc);
            container.addMapping(WEBSOCKET_WITHOUT_FILTER_PATH, jwsc);
            ensureFilter(servletContext, WEBSOCKET_WITH_FILTER_PATH, StripContentLengthOnConnectFilter.class);
          });
    }
  }

  private static void ensureFilter(
      final ServletContext servletContext,
      final String pathSpec,
      final Class<? extends Filter> filterClass) {

    final ContextHandler contextHandler = ServletContextHandler.getServletContextHandler(servletContext);
    final ServletHandler servletHandler = contextHandler.getDescendant(ServletHandler.class);
    final FilterHolder holder = new FilterHolder(filterClass);
    final FilterMapping mapping = new FilterMapping();
    mapping.setFilterName(holder.getName());
    mapping.setPathSpec(pathSpec);
    mapping.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));
    servletHandler.prependFilter(holder);
    servletHandler.prependFilterMapping(mapping);
  }
}
