package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.uri.UriTemplate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsRequestEventListenerTest {

    private              MeterRegistry               meterRegistry;
    private              Counter                     counter;

    private              MetricsRequestEventListener listener;

    private static final TrafficSource               TRAFFIC_SOURCE = TrafficSource.HTTP;

    @Before
    public void setup() {
        meterRegistry = mock(MeterRegistry.class);
        counter       = mock(Counter.class);

        listener      = new MetricsRequestEventListener(TRAFFIC_SOURCE, meterRegistry);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnEvent() {
        final String path = "/test";
        final int statusCode = 200;

        final ExtendedUriInfo uriInfo = mock(ExtendedUriInfo.class);
        when(uriInfo.getMatchedTemplates()).thenReturn(Collections.singletonList(new UriTemplate(path)));

        final ContainerRequest request = mock(ContainerRequest.class);
        when(request.getRequestHeader("User-Agent")).thenReturn(Collections.singletonList("Signal-Android 4.53.7 (Android 8.1)"));

        final ContainerResponse response = mock(ContainerResponse.class);
        when(response.getStatus()).thenReturn(statusCode);

        final RequestEvent event = mock(RequestEvent.class);
        when(event.getType()).thenReturn(RequestEvent.Type.FINISHED);
        when(event.getUriInfo()).thenReturn(uriInfo);
        when(event.getContainerRequest()).thenReturn(request);
        when(event.getContainerResponse()).thenReturn(response);

        final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);
        when(meterRegistry.counter(eq(MetricsRequestEventListener.COUNTER_NAME), any(Iterable.class))).thenReturn(counter);

        listener.onEvent(event);

        verify(meterRegistry).counter(eq(MetricsRequestEventListener.COUNTER_NAME), tagCaptor.capture());

        final Iterable<Tag> tagIterable = tagCaptor.getValue();
        final Set<Tag> tags = new HashSet<>();

        for (final Tag tag : tagIterable) {
            tags.add(tag);
        }

        assertEquals(5, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PATH_TAG, path)));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.STATUS_CODE_TAG, String.valueOf(statusCode))));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.TRAFFIC_SOURCE_TAG, TRAFFIC_SOURCE.name().toLowerCase())));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "4.53.7")));
    }

    @Test
    public void testGetPathTemplate() {
        final UriTemplate firstComponent = new UriTemplate("/first");
        final UriTemplate secondComponent = new UriTemplate("/second");
        final UriTemplate thirdComponent = new UriTemplate("/{param}/{moreDifferentParam}");

        final ExtendedUriInfo uriInfo = mock(ExtendedUriInfo.class);
        when(uriInfo.getMatchedTemplates()).thenReturn(Arrays.asList(thirdComponent, secondComponent, firstComponent));

        assertEquals("/first/second/{param}/{moreDifferentParam}", MetricsRequestEventListener.getPathTemplate(uriInfo));
    }
}
