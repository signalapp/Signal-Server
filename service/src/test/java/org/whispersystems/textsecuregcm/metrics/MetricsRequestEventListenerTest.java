package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.bouncycastle.ocsp.Req;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.uri.UriTemplate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsRequestEventListenerTest {

    private MeterRegistry               meterRegistry;
    private Counter                     counter;

    private MetricsRequestEventListener listener;

    @Before
    public void setup() {
        meterRegistry = mock(MeterRegistry.class);
        counter       = mock(Counter.class);

        listener      = new MetricsRequestEventListener(meterRegistry);
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

        assertEquals(4, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PATH_TAG, path)));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.STATUS_CODE_TAG, String.valueOf(statusCode))));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PLATFORM_TAG, "android")));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.VERSION_TAG, "4.53.7")));
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

    @Test
    public void testGetUserAgentTags() {
        assertEquals(MetricsRequestEventListener.UNRECOGNIZED_TAGS,
                listener.getUserAgentTags("This is obviously not a reasonable User-Agent string."));

        final List<Tag> tags = listener.getUserAgentTags("Signal-Android 4.53.7 (Android 8.1)");

        assertEquals(2, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PLATFORM_TAG, "android")));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.VERSION_TAG, "4.53.7")));
    }

    @Test
    public void testGetUserAgentTagsFlooded() {
        for (int i = 0; i < MetricsRequestEventListener.MAX_VERSIONS; i++) {
            listener.getUserAgentTags(String.format("Signal-Android 1.0.%d (Android 8.1)", i));
        }

        assertEquals(MetricsRequestEventListener.OVERFLOW_TAGS,
                listener.getUserAgentTags("Signal-Android 2.0.0 (Android 8.1)"));

        final List<Tag> tags = listener.getUserAgentTags("Signal-Android 1.0.0 (Android 8.1)");

        assertEquals(2, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PLATFORM_TAG, "android")));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.VERSION_TAG, "1.0.0")));
    }
}
