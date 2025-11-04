/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.uri.UriTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;

class MetricsHttpEventHandlerTest {
  private final static String USER_AGENT = "Signal-Android/6.53.7 (Android 8.1)";

  private MeterRegistry meterRegistry;
  private Counter requestCounter;
  private Counter requestsByVersionCounter;
  private Counter responseBytesCounter;
  private Counter requestBytesCounter;
  private ClientReleaseManager clientReleaseManager;
  private MetricsHttpEventHandler listener;

  @BeforeEach
  void setup() {
    meterRegistry = mock(MeterRegistry.class);
    requestCounter = mock(Counter.class);
    requestsByVersionCounter = mock(Counter.class);
    responseBytesCounter = mock(Counter.class);
    requestBytesCounter = mock(Counter.class);

    when(meterRegistry.counter(eq(MetricsHttpEventHandler.REQUEST_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(requestCounter);

    when(meterRegistry.counter(eq(MetricsHttpEventHandler.REQUESTS_BY_VERSION_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(requestsByVersionCounter);

    when(meterRegistry.counter(eq(MetricsHttpEventHandler.RESPONSE_BYTES_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(responseBytesCounter);

    when(meterRegistry.counter(eq(MetricsHttpEventHandler.REQUEST_BYTES_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(requestBytesCounter);

    clientReleaseManager = mock(ClientReleaseManager.class);

    listener = new MetricsHttpEventHandler(null, meterRegistry, clientReleaseManager, Set.of("/test"));
  }

  @CartesianTest
  @SuppressWarnings("unchecked")
  void testRequests(@CartesianTest.Values(booleans = {true, false}) final boolean pathFromFilter,
      @CartesianTest.Values(booleans = {true, false}) final boolean versionActive) {

    final String path = "/test";
    final String method = "GET";
    final int statusCode = 200;

    final HttpURI httpUri = mock(HttpURI.class);
    when(httpUri.getPath()).thenReturn(path);

    final Request request = mock(Request.class);
    when(request.getMethod()).thenReturn(method);

    final HttpFields.Mutable requestHeaders = HttpFields.build();
    requestHeaders.put(HttpHeader.USER_AGENT, USER_AGENT);
    when(request.getHeaders()).thenReturn(requestHeaders);
    when(request.getHttpURI()).thenReturn(httpUri);

    if (pathFromFilter) {
      when(request.getAttribute(MetricsHttpEventHandler.REQUEST_INFO_PROPERTY_NAME))
          .thenReturn(new MetricsHttpEventHandler.RequestInfo(path, method, USER_AGENT));
    } else {
      when(request.setAttribute(eq(MetricsHttpEventHandler.REQUEST_INFO_PROPERTY_NAME), any())).thenAnswer(invocation -> {
        when(request.getAttribute(MetricsHttpEventHandler.REQUEST_INFO_PROPERTY_NAME))
            .thenReturn(invocation.getArgument(1));
        return null;
      });
    }

    final Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(statusCode);

    when(clientReleaseManager.isVersionActive(any(), any())).thenReturn(versionActive);

    final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);

    listener.onRequestRead(request, Content.Chunk.from(ByteBuffer.allocate(512), true));
    listener.onResponseWrite(request, true, ByteBuffer.allocate(1024));
    listener.onComplete(request, statusCode, requestHeaders, null);

    verify(requestCounter).increment();

    verify(responseBytesCounter).increment(1024L);
    verify(requestBytesCounter).increment(512L);

    verify(meterRegistry).counter(eq(MetricsHttpEventHandler.REQUEST_COUNTER_NAME), tagCaptor.capture());

    final Set<Tag> tags = new HashSet<>();
    for (final Tag tag : tagCaptor.getValue()) {
      tags.add(tag);
    }

    assertEquals(versionActive ? 7 : 6, tags.size());
    assertTrue(tags.contains(Tag.of(MetricsHttpEventHandler.PATH_TAG, path)));
    assertTrue(tags.contains(Tag.of(MetricsHttpEventHandler.METHOD_TAG, method)));
    assertTrue(tags.contains(Tag.of(MetricsHttpEventHandler.STATUS_CODE_TAG, String.valueOf(statusCode))));
    assertTrue(
        tags.contains(Tag.of(MetricsHttpEventHandler.TRAFFIC_SOURCE_TAG, TrafficSource.HTTP.name().toLowerCase())));
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.LIBSIGNAL_TAG, "false")));
    assertEquals(versionActive, tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "6.53.7")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @SuppressWarnings("unchecked")
  void testRequestsByVersion(final boolean versionActive) {
    when(clientReleaseManager.isVersionActive(any(), any())).thenReturn(versionActive);
    final String path = "/test";
    final String method = "GET";
    final int statusCode = 200;

    final HttpURI httpUri = mock(HttpURI.class);
    when(httpUri.getPath()).thenReturn(path);

    final Request request = mock(Request.class);
    when(request.getMethod()).thenReturn(method);
    final HttpFields.Mutable requestHeaders = HttpFields.build();
    requestHeaders.put(HttpHeader.USER_AGENT, USER_AGENT);
    when(request.getHeaders()).thenReturn(requestHeaders);
    when(request.getHttpURI()).thenReturn(httpUri);
    when(request.getAttribute(MetricsHttpEventHandler.REQUEST_INFO_PROPERTY_NAME))
        .thenReturn(new MetricsHttpEventHandler.RequestInfo(path, method, USER_AGENT));

    final Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(statusCode);
    listener.onRequestRead(request, Content.Chunk.from(ByteBuffer.allocate(512), true));
    listener.onResponseWrite(request, true, ByteBuffer.allocate(1024));
    listener.onComplete(request, statusCode, requestHeaders, null);

    if (versionActive) {
      final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);
      verify(meterRegistry).counter(eq(MetricsHttpEventHandler.REQUESTS_BY_VERSION_COUNTER_NAME),
          tagCaptor.capture());
      final Set<Tag> tags = new HashSet<>();
      tags.clear();
      for (final Tag tag : tagCaptor.getValue()) {
        tags.add(tag);
      }

      assertEquals(2, tags.size());
      assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "6.53.7")));
      assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
    } else {
      verifyNoInteractions(requestsByVersionCounter);
    }
  }

  @Test
  void testResponseFilterSetsRequestInfo() {
    final ContainerRequest request = mock(ContainerRequest.class);

    final ExtendedUriInfo extendedUriInfo = mock(ExtendedUriInfo.class);
    when(extendedUriInfo.getMatchedTemplates()).thenReturn(List.of(new UriTemplate("/test")));
    when(request.getMethod()).thenReturn("GET");
    when(request.getHeaders()).thenReturn(null);
    when(request.getUriInfo()).thenReturn(extendedUriInfo);
    when(request.getHeaderString(HttpHeaders.USER_AGENT)).thenReturn(USER_AGENT);

    new MetricsHttpEventHandler.SetInfoRequestFilter().filter(request, mock(ContainerResponse.class));

    verify(request).setProperty(
        eq(MetricsHttpEventHandler.REQUEST_INFO_PROPERTY_NAME),
        eq(new MetricsHttpEventHandler.RequestInfo("/test", "GET", USER_AGENT)));
  }

  @Test
  void testResponseFilterModifiesRequestInfo() {
    final MetricsHttpEventHandler.RequestInfo requestInfo =
        new MetricsHttpEventHandler.RequestInfo("unknown", "POST", USER_AGENT);

    final ContainerRequest request = mock(ContainerRequest.class);
    when(request.getProperty(MetricsHttpEventHandler.REQUEST_INFO_PROPERTY_NAME)).thenReturn(requestInfo);
    final ExtendedUriInfo extendedUriInfo = mock(ExtendedUriInfo.class);
    when(extendedUriInfo.getMatchedTemplates()).thenReturn(List.of(new UriTemplate("/test")));
    when(request.getUriInfo()).thenReturn(extendedUriInfo);
    new MetricsHttpEventHandler.SetInfoRequestFilter().filter(request, mock(ContainerResponse.class));

    assertEquals(new MetricsHttpEventHandler.RequestInfo("/test", "POST", USER_AGENT), requestInfo);
  }
}
