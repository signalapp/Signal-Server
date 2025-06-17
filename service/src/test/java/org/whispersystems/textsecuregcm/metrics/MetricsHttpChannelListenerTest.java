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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.uri.UriTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;

class MetricsHttpChannelListenerTest {

  private MeterRegistry meterRegistry;
  private Counter requestCounter;
  private Counter requestsByVersionCounter;
  private Counter responseBytesCounter;
  private Counter requestBytesCounter;
  private ClientReleaseManager clientReleaseManager;
  private MetricsHttpChannelListener listener;

  @BeforeEach
  void setup() {
    meterRegistry = mock(MeterRegistry.class);
    requestCounter = mock(Counter.class);
    requestsByVersionCounter = mock(Counter.class);
    responseBytesCounter = mock(Counter.class);
    requestBytesCounter = mock(Counter.class);

    when(meterRegistry.counter(eq(MetricsHttpChannelListener.REQUEST_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(requestCounter);

    when(meterRegistry.counter(eq(MetricsHttpChannelListener.REQUESTS_BY_VERSION_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(requestsByVersionCounter);

    when(meterRegistry.counter(eq(MetricsHttpChannelListener.RESPONSE_BYTES_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(responseBytesCounter);

    when(meterRegistry.counter(eq(MetricsHttpChannelListener.REQUEST_BYTES_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(requestBytesCounter);

    clientReleaseManager = mock(ClientReleaseManager.class);

    listener = new MetricsHttpChannelListener(meterRegistry, clientReleaseManager, Collections.emptySet());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testRequests() {
    final String path = "/test";
    final String method = "GET";
    final int statusCode = 200;

    final HttpURI httpUri = mock(HttpURI.class);
    when(httpUri.getPath()).thenReturn(path);

    final Request request = mock(Request.class);
    when(request.getMethod()).thenReturn(method);
    when(request.getHeader(HttpHeaders.USER_AGENT)).thenReturn("Signal-Android/4.53.7 (Android 8.1)");
    when(request.getHttpURI()).thenReturn(httpUri);

    final Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(statusCode);
    when(response.getContentCount()).thenReturn(1024L);
    when(request.getResponse()).thenReturn(response);
    when(request.getContentRead()).thenReturn(512L);
    final ExtendedUriInfo extendedUriInfo = mock(ExtendedUriInfo.class);
    when(request.getAttribute(MetricsHttpChannelListener.URI_INFO_PROPERTY_NAME)).thenReturn(extendedUriInfo);
    when(extendedUriInfo.getMatchedTemplates()).thenReturn(List.of(new UriTemplate(path)));

    final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);

    listener.onComplete(request);

    verify(requestCounter).increment();

    verify(responseBytesCounter).increment(1024L);
    verify(requestBytesCounter).increment(512L);

    verify(meterRegistry).counter(eq(MetricsHttpChannelListener.REQUEST_COUNTER_NAME), tagCaptor.capture());

    final Set<Tag> tags = new HashSet<>();
    for (final Tag tag : tagCaptor.getValue()) {
      tags.add(tag);
    }

    assertEquals(6, tags.size());
    assertTrue(tags.contains(Tag.of(MetricsHttpChannelListener.PATH_TAG, path)));
    assertTrue(tags.contains(Tag.of(MetricsHttpChannelListener.METHOD_TAG, method)));
    assertTrue(tags.contains(Tag.of(MetricsHttpChannelListener.STATUS_CODE_TAG, String.valueOf(statusCode))));
    assertTrue(
        tags.contains(Tag.of(MetricsHttpChannelListener.TRAFFIC_SOURCE_TAG, TrafficSource.HTTP.name().toLowerCase())));
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.LIBSIGNAL_TAG, "false")));
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
    when(request.getHeader(HttpHeaders.USER_AGENT)).thenReturn("Signal-Android/6.53.7 (Android 8.1)");
    when(request.getHttpURI()).thenReturn(httpUri);

    final Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(statusCode);
    when(response.getContentCount()).thenReturn(1024L);
    when(request.getResponse()).thenReturn(response);
    when(request.getContentRead()).thenReturn(512L);
    final ExtendedUriInfo extendedUriInfo = mock(ExtendedUriInfo.class);
    when(request.getAttribute(MetricsHttpChannelListener.URI_INFO_PROPERTY_NAME)).thenReturn(extendedUriInfo);
    when(extendedUriInfo.getMatchedTemplates()).thenReturn(List.of(new UriTemplate(path)));

    listener.onComplete(request);

    if (versionActive) {
      final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);
      verify(meterRegistry).counter(eq(MetricsHttpChannelListener.REQUESTS_BY_VERSION_COUNTER_NAME),
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
}
