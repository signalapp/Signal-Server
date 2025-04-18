/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import com.vdurmont.semver4j.Semver;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRemoteDeprecationConfiguration;
import org.whispersystems.textsecuregcm.grpc.EchoServiceImpl;
import org.whispersystems.textsecuregcm.grpc.MockRequestAttributesInterceptor;
import org.whispersystems.textsecuregcm.grpc.RequestAttributes;
import org.whispersystems.textsecuregcm.grpc.StatusConstants;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

class RemoteDeprecationFilterTest {

  @Test
  void testEmptyMap() throws IOException, ServletException {
    // We're happy as long as there's no exception
    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicRemoteDeprecationConfiguration emptyConfiguration = new DynamicRemoteDeprecationConfiguration();

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getRemoteDeprecationConfiguration()).thenReturn(emptyConfiguration);

    final RemoteDeprecationFilter filter = new RemoteDeprecationFilter(dynamicConfigurationManager);

    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    final FilterChain filterChain = mock(FilterChain.class);

    when(servletRequest.getHeader("UserAgent")).thenReturn("Signal-Android/4.68.3");

    filter.doFilter(servletRequest, servletResponse, filterChain);

    verify(filterChain).doFilter(servletRequest, servletResponse);
    verify(servletResponse, never()).sendError(anyInt());
  }

  private RemoteDeprecationFilter filterConfiguredForTest() {
    final EnumMap<ClientPlatform, Semver> minimumVersionsByPlatform = new EnumMap<>(ClientPlatform.class);
    minimumVersionsByPlatform.put(ClientPlatform.ANDROID, new Semver("1.0.0"));
    minimumVersionsByPlatform.put(ClientPlatform.IOS, new Semver("1.0.0"));
    minimumVersionsByPlatform.put(ClientPlatform.DESKTOP, new Semver("1.0.0"));

    final EnumMap<ClientPlatform, Semver> versionsPendingDeprecationByPlatform = new EnumMap<>(ClientPlatform.class);
    minimumVersionsByPlatform.put(ClientPlatform.ANDROID, new Semver("1.1.0"));
    minimumVersionsByPlatform.put(ClientPlatform.IOS, new Semver("1.1.0"));
    minimumVersionsByPlatform.put(ClientPlatform.DESKTOP, new Semver("1.1.0"));

    final EnumMap<ClientPlatform, Set<Semver>> blockedVersionsByPlatform = new EnumMap<>(ClientPlatform.class);
    blockedVersionsByPlatform.put(ClientPlatform.DESKTOP, Set.of(new Semver("8.0.0-beta.2")));

    final EnumMap<ClientPlatform, Set<Semver>> versionsPendingBlockByPlatform = new EnumMap<>(ClientPlatform.class);
    versionsPendingBlockByPlatform.put(ClientPlatform.DESKTOP, Set.of(new Semver("8.0.0-beta.3")));

    final DynamicRemoteDeprecationConfiguration remoteDeprecationConfiguration = new DynamicRemoteDeprecationConfiguration();
    remoteDeprecationConfiguration.setMinimumVersions(minimumVersionsByPlatform);
    remoteDeprecationConfiguration.setVersionsPendingDeprecation(versionsPendingDeprecationByPlatform);
    remoteDeprecationConfiguration.setBlockedVersions(blockedVersionsByPlatform);
    remoteDeprecationConfiguration.setVersionsPendingBlock(versionsPendingBlockByPlatform);
    remoteDeprecationConfiguration.setUnrecognizedUserAgentAllowed(true);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getRemoteDeprecationConfiguration()).thenReturn(remoteDeprecationConfiguration);

    return new RemoteDeprecationFilter(dynamicConfigurationManager);
  }

  @ParameterizedTest
  @MethodSource
  void testFilter(final String userAgent, final boolean expectDeprecation) throws IOException, ServletException {
    final HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    final HttpServletResponse servletResponse = mock(HttpServletResponse.class);
    final FilterChain filterChain = mock(FilterChain.class);

    when(servletRequest.getHeader(HttpHeaders.USER_AGENT)).thenReturn(userAgent);

    final RemoteDeprecationFilter filter = filterConfiguredForTest();
    filter.doFilter(servletRequest, servletResponse, filterChain);

    if (expectDeprecation) {
      verify(filterChain, never()).doFilter(any(), any());
      verify(servletResponse).sendError(499);
    } else {
      verify(filterChain).doFilter(servletRequest, servletResponse);
      verify(servletResponse, never()).sendError(anyInt());
    }
  }

  @ParameterizedTest
  @MethodSource(value="testFilter")
  void testGrpcFilter(final String userAgentString, final boolean expectDeprecation) throws IOException, InterruptedException {
    final MockRequestAttributesInterceptor mockRequestAttributesInterceptor = new MockRequestAttributesInterceptor();
    mockRequestAttributesInterceptor.setRequestAttributes(new RequestAttributes(InetAddresses.forString("127.0.0.1"), userAgentString, null));

    final Server testServer = InProcessServerBuilder.forName("RemoteDeprecationFilterTest")
        .directExecutor()
        .addService(new EchoServiceImpl())
        .intercept(filterConfiguredForTest())
        .intercept(mockRequestAttributesInterceptor)
        .build()
        .start();

    final ManagedChannel channel = InProcessChannelBuilder.forName("RemoteDeprecationFilterTest")
        .directExecutor()
        .userAgent(userAgentString)
        .build();

    try {
      final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);

      final EchoRequest req = EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("cluck cluck, i'm a parrot")).build();
      if (expectDeprecation) {
        final StatusRuntimeException e = assertThrows(
            StatusRuntimeException.class,
            () -> client.echo(req));
        assertEquals(StatusConstants.UPGRADE_NEEDED_STATUS.toString(), e.getStatus().toString());
      } else {
        assertEquals("cluck cluck, i'm a parrot", client.echo(req).getPayload().toStringUtf8());
      }
    } finally {
      testServer.shutdownNow();
      testServer.awaitTermination();
    }
  }

  private static Stream<Arguments> testFilter() {
    return Stream.of(
        Arguments.of("Unrecognized UA", false),
        Arguments.of("Signal-Android/4.68.3", false),
        Arguments.of("Signal-iOS/3.9.0", false),
        Arguments.of("Signal-Desktop/1.2.3", false),
        Arguments.of("Signal-Android/0.68.3", true),
        Arguments.of("Signal-iOS/0.9.0", true),
        Arguments.of("Signal-Desktop/0.2.3", true),
        Arguments.of("Signal-Desktop/8.0.0-beta.2", true),
        Arguments.of("Signal-Desktop/8.0.0-beta.1", false),
        Arguments.of("Signal-iOS/8.0.0-beta.2", false));
  }

}
