/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vdurmont.semver4j.Semver;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.uri.UriTemplate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.SubProtocol;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyVararg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class MetricsRequestEventListenerTest {

    private MeterRegistry               meterRegistry;
    private Counter                     counter;
    private MetricsRequestEventListener listener;

    private static final TrafficSource TRAFFIC_SOURCE = TrafficSource.HTTP;

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
        when(meterRegistry.counter(eq(MetricsRequestEventListener.REQUEST_COUNTER_NAME), any(Iterable.class))).thenReturn(counter);

        listener.onEvent(event);

        verify(meterRegistry).counter(eq(MetricsRequestEventListener.REQUEST_COUNTER_NAME), tagCaptor.capture());

        final Iterable<Tag> tagIterable = tagCaptor.getValue();
        final Set<Tag> tags = new HashSet<>();

        for (final Tag tag : tagIterable) {
            tags.add(tag);
        }

        // TODO Restore this when we return to detailed metrics and restore the version tag
        // assertEquals(5, tags.size());
        assertEquals(4, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PATH_TAG, path)));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.STATUS_CODE_TAG, String.valueOf(statusCode))));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.TRAFFIC_SOURCE_TAG, TRAFFIC_SOURCE.name().toLowerCase())));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
        // assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "4.53.7")));
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
    public void testActualRouteMessageSuccess() throws InvalidProtocolBufferException {
        MetricsApplicationEventListener applicationEventListener = mock(MetricsApplicationEventListener.class);
        when(applicationEventListener.onRequest(any())).thenReturn(listener);

        ResourceConfig resourceConfig = new DropwizardResourceConfig();
        resourceConfig.register(applicationEventListener);
        resourceConfig.register(new TestResource());
        resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
        resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
        resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

        ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
        WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
        WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

        Session        session        = mock(Session.class       );
        RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
        UpgradeRequest request        = mock(UpgradeRequest.class);

        when(session.getUpgradeRequest()).thenReturn(request);
        when(session.getRemote()).thenReturn(remoteEndpoint);
        when(request.getHeader("User-Agent")).thenReturn("Signal-Android 4.53.7 (Android 8.1)");

        final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);
        when(meterRegistry.counter(eq(MetricsRequestEventListener.REQUEST_COUNTER_NAME), any(Iterable.class))).thenReturn(counter);

        provider.onWebSocketConnect(session);

        byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/hello", new LinkedList<>(), Optional.empty()).toByteArray();

        provider.onWebSocketBinary(message, 0, message.length);

        ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

        SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

        assertThat(response.getStatus()).isEqualTo(200);

        verify(meterRegistry).counter(eq(MetricsRequestEventListener.REQUEST_COUNTER_NAME), tagCaptor.capture());

        final Iterable<Tag> tagIterable = tagCaptor.getValue();
        final Set<Tag> tags = new HashSet<>();

        for (final Tag tag : tagIterable) {
            tags.add(tag);
        }

        // TODO Restore this when we return to detailed metrics and restore the version tag
        // assertEquals(5, tags.size());
        assertEquals(4, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PATH_TAG, "/v1/test/hello")));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.STATUS_CODE_TAG, String.valueOf(200))));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.TRAFFIC_SOURCE_TAG, TRAFFIC_SOURCE.name().toLowerCase())));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
        // assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "4.53.7")));
    }

    @Test
    public void testActualRouteMessageSuccessNoUserAgent() throws InvalidProtocolBufferException {
        MetricsApplicationEventListener applicationEventListener = mock(MetricsApplicationEventListener.class);
        when(applicationEventListener.onRequest(any())).thenReturn(listener);

        ResourceConfig resourceConfig = new DropwizardResourceConfig();
        resourceConfig.register(applicationEventListener);
        resourceConfig.register(new TestResource());
        resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
        resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
        resourceConfig.register(new JacksonMessageBodyProvider(new ObjectMapper()));

        ApplicationHandler                       applicationHandler = new ApplicationHandler(resourceConfig);
        WebsocketRequestLog                      requestLog         = mock(WebsocketRequestLog.class);
        WebSocketResourceProvider<TestPrincipal> provider           = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler, requestLog, new TestPrincipal("foo"), new ProtobufWebSocketMessageFactory(), Optional.empty(), 30000);

        Session        session        = mock(Session.class       );
        RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
        UpgradeRequest request        = mock(UpgradeRequest.class);

        when(session.getUpgradeRequest()).thenReturn(request);
        when(session.getRemote()).thenReturn(remoteEndpoint);

        final ArgumentCaptor<Iterable<Tag>> tagCaptor = ArgumentCaptor.forClass(Iterable.class);
        when(meterRegistry.counter(eq(MetricsRequestEventListener.REQUEST_COUNTER_NAME), any(Iterable.class))).thenReturn(counter);

        provider.onWebSocketConnect(session);

        byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/hello", new LinkedList<>(), Optional.empty()).toByteArray();

        provider.onWebSocketBinary(message, 0, message.length);

        ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

        SubProtocol.WebSocketResponseMessage response = getResponse(responseBytesCaptor);

        assertThat(response.getStatus()).isEqualTo(200);

        verify(meterRegistry).counter(eq(MetricsRequestEventListener.REQUEST_COUNTER_NAME), tagCaptor.capture());

        final Iterable<Tag> tagIterable = tagCaptor.getValue();
        final Set<Tag> tags = new HashSet<>();

        for (final Tag tag : tagIterable) {
            tags.add(tag);
        }

        // TODO Restore this when we return to detailed metrics and restore the version tag
        // assertEquals(5, tags.size());
        assertEquals(4, tags.size());
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.PATH_TAG, "/v1/test/hello")));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.STATUS_CODE_TAG, String.valueOf(200))));
        assertTrue(tags.contains(Tag.of(MetricsRequestEventListener.TRAFFIC_SOURCE_TAG, TRAFFIC_SOURCE.name().toLowerCase())));
        // assertTrue(tags.containsAll(UserAgentTagUtil.UNRECOGNIZED_TAGS));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized")));
    }

    @Test
    @Parameters(method = "argumentsForTestRecordDesktopOperatingSystem")
    public void testRecordDesktopOperatingSystem(final UserAgent userAgent, final String expectedOperatingSystem) {
        when(meterRegistry.counter(eq(MetricsRequestEventListener.DESKTOP_REQUEST_COUNTER_NAME), (String)any())).thenReturn(counter);
        listener.recordDesktopOperatingSystem(userAgent);

        if (expectedOperatingSystem != null) {
            final ArgumentCaptor<String> tagCaptor = ArgumentCaptor.forClass(String.class);
            verify(meterRegistry).counter(eq(MetricsRequestEventListener.DESKTOP_REQUEST_COUNTER_NAME), tagCaptor.capture());

            assertEquals(List.of(MetricsRequestEventListener.OS_TAG, expectedOperatingSystem), tagCaptor.getAllValues());
        } else {
            verify(meterRegistry, never()).counter(eq(MetricsRequestEventListener.DESKTOP_REQUEST_COUNTER_NAME));
            verify(meterRegistry, never()).counter(eq(MetricsRequestEventListener.DESKTOP_REQUEST_COUNTER_NAME), (String)any());
        }
    }

    private static Object argumentsForTestRecordDesktopOperatingSystem() {
        return new Object[] {
                new Object[] { new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux"),                      "linux" },
                new Object[] { new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "macOS"),                      "macos" },
                new Object[] { new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Windows"),                    "windows" },
                new Object[] { new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3")),                               null },
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25"),                null },
                new Object[] { new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)"), null },
        };
    }

    @Test
    @Parameters(method = "argumentsForTestRecordAndroidSdkVersion")
    public void testRecordAndroidSdkVersion(final UserAgent userAgent, final String expectedSdkVersion) {
        when(meterRegistry.counter(eq(MetricsRequestEventListener.ANDROID_REQUEST_COUNTER_NAME), (String)any())).thenReturn(counter);
        listener.recordAndroidSdkVersion(userAgent);

        if (expectedSdkVersion != null) {
            final ArgumentCaptor<String> tagCaptor = ArgumentCaptor.forClass(String.class);
            verify(meterRegistry).counter(eq(MetricsRequestEventListener.ANDROID_REQUEST_COUNTER_NAME), tagCaptor.capture());

            assertEquals(List.of(MetricsRequestEventListener.SDK_TAG, expectedSdkVersion), tagCaptor.getAllValues());
        } else {
            verify(meterRegistry, never()).counter(eq(MetricsRequestEventListener.ANDROID_REQUEST_COUNTER_NAME));
            verify(meterRegistry, never()).counter(eq(MetricsRequestEventListener.ANDROID_REQUEST_COUNTER_NAME), (String)any());
        }
    }

    private static Object argumentsForTestRecordAndroidSdkVersion() {
        return new Object[] {
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/1"),                     null },
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25"),                    "25" },
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/700000"),                null },
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/"),                      null },
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), null),                            null },
                new Object[] { new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux"),                          null },
                new Object[] { new UserAgent(ClientPlatform.IOS,     new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)"), null }
        };
    }

    @Test
    @Parameters(method = "argumentsForTestRecordIosVersion")
    public void testRecordIosVersion(final UserAgent userAgent, final String expectedIosVersion) {
        when(meterRegistry.counter(eq(MetricsRequestEventListener.IOS_REQUEST_COUNTER_NAME), (String)any())).thenReturn(counter);
        listener.recordIosVersion(userAgent);

        if (expectedIosVersion != null) {
            final ArgumentCaptor<String> tagCaptor = ArgumentCaptor.forClass(String.class);
            verify(meterRegistry).counter(eq(MetricsRequestEventListener.IOS_REQUEST_COUNTER_NAME), tagCaptor.capture());

            assertEquals(List.of(MetricsRequestEventListener.OS_TAG, expectedIosVersion), tagCaptor.getAllValues());
        } else {
            verify(meterRegistry, never()).counter(eq(MetricsRequestEventListener.IOS_REQUEST_COUNTER_NAME));
            verify(meterRegistry, never()).counter(eq(MetricsRequestEventListener.IOS_REQUEST_COUNTER_NAME), (String)any());
        }
    }

    private static Object argumentsForTestRecordIosVersion() {
        return new Object[] {
                new Object[] { new UserAgent(ClientPlatform.IOS,     new Semver("3.9.0"), "iOS/14.2"),                        "14.2" },
                new Object[] { new UserAgent(ClientPlatform.IOS,     new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)"),  "12.2" },
                new Object[] { new UserAgent(ClientPlatform.IOS,     new Semver("3.9.0")),                                    null },
                new Object[] { new UserAgent(ClientPlatform.IOS,     new Semver("3.9.0"), "iOS/bogus"),                       null },
                new Object[] { new UserAgent(ClientPlatform.IOS,     new Semver("3.9.0"), "(iPhone; iOS bogus; Scale/3.00)"), null },
                new Object[] { new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25"),                     null },
                new Object[] { new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux"),                           null }
        };
    }

    private static SubProtocol.WebSocketResponseMessage getResponse(ArgumentCaptor<ByteBuffer> responseCaptor) throws InvalidProtocolBufferException {
        return SubProtocol.WebSocketMessage.parseFrom(responseCaptor.getValue().array()).getResponse();
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

    @Path("/v1/test")
    public static class TestResource {

        @GET
        @Path("/hello")
        public String testGetHello() {
            return "Hello!";
        }
    }
}
