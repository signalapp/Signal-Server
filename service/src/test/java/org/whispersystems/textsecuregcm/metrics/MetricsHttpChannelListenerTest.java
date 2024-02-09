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
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
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
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;

class MetricsHttpChannelListenerTest {

  private MeterRegistry meterRegistry;
  private Counter counter;
  private MetricsHttpChannelListener listener;

  @BeforeEach
  void setup() {
    meterRegistry = mock(MeterRegistry.class);
    counter = mock(Counter.class);

    final ClientReleaseManager clientReleaseManager = mock(ClientReleaseManager.class);
    when(clientReleaseManager.isVersionActive(any(), any())).thenReturn(false);

    listener = new MetricsHttpChannelListener(meterRegistry, clientReleaseManager);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testOnEvent() {
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
    when(request.getResponse()).thenReturn(response);
    final ExtendedUriInfo extendedUriInfo = mock(ExtendedUriInfo.class);
    when(request.getAttribute(MetricsHttpChannelListener.URI_INFO_PROPERTY_NAME)).thenReturn(extendedUriInfo);
    when(extendedUriInfo.getMatchedTemplates()).thenReturn(List.of(new UriTemplate(path)));

    final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);
    when(meterRegistry.counter(eq(MetricsHttpChannelListener.REQUEST_COUNTER_NAME), any(Iterable.class)))
        .thenReturn(counter);

    listener.onComplete(request);

    verify(meterRegistry).counter(eq(MetricsHttpChannelListener.REQUEST_COUNTER_NAME), tagCaptor.capture());

    final Iterable<Tag> tagIterable = tagCaptor.getValue();
    final Set<Tag> tags = new HashSet<>();

    for (final Tag tag : tagIterable) {
      tags.add(tag);
    }

    assertEquals(5, tags.size());
    assertTrue(tags.contains(Tag.of(MetricsHttpChannelListener.PATH_TAG, path)));
    assertTrue(tags.contains(Tag.of(MetricsHttpChannelListener.METHOD_TAG, method)));
    assertTrue(tags.contains(Tag.of(MetricsHttpChannelListener.STATUS_CODE_TAG, String.valueOf(statusCode))));
    assertTrue(
        tags.contains(Tag.of(MetricsHttpChannelListener.TRAFFIC_SOURCE_TAG, TrafficSource.HTTP.name().toLowerCase())));
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
  }
}
