/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.BlockingClientCall;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class MetricServerInterceptorTest {

  private static String USER_AGENT = "Signal-Android/4.53.7 (Android 8.1; libsignal)";

  private Server server;
  private ManagedChannel channel;
  private SimpleMeterRegistry simpleMeterRegistry;
  private ClientReleaseManager clientReleaseManager;

  @BeforeEach
  void setUp() throws Exception {
    simpleMeterRegistry = new SimpleMeterRegistry();
    clientReleaseManager = mock(ClientReleaseManager.class);
    final MockRequestAttributesInterceptor mockRequestAttributesInterceptor = new MockRequestAttributesInterceptor();
    mockRequestAttributesInterceptor.setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), USER_AGENT, null));

    server = InProcessServerBuilder.forName("MetricServerInterceptorTest")
        .directExecutor()
        .addService(new EchoServiceImpl())
        .intercept(new MetricServerInterceptor(simpleMeterRegistry, clientReleaseManager))
        .intercept(mockRequestAttributesInterceptor)
        .intercept(mockRequestAttributesInterceptor)
        .build()
        .start();

    channel = InProcessChannelBuilder.forName("MetricServerInterceptorTest")
        .directExecutor()
        .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.shutdownNow();
    channel.shutdownNow();
    server.awaitTermination(1, TimeUnit.SECONDS);
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void unary() {
    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    client.echo(EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("hello")).build());

    final Tags commonTags = Tags.of(
        "platform", "android",
        "libsignal", "true",
        "grpcService", "org.signal.chat.rpc.EchoService",
        "method", "echo");

    final Counter requestCount = find(Counter.class, MetricServerInterceptor.REQUEST_MESSAGE_COUNTER_NAME);
    assertThat(requestCount.count()).isCloseTo(1.0, offset(0.01));

    final Counter responseCount = find(Counter.class, MetricServerInterceptor.RESPONSE_COUNTER_NAME);
    assertThat(responseCount.count()).isCloseTo(1.0, offset(0.01));

    final Counter rpcCount = find(Counter.class, MetricServerInterceptor.RPC_COUNTER_NAME);
    assertThat(rpcCount.count()).isCloseTo(1.0, offset(0.01));

    final Timer timer = find(Timer.class, MetricServerInterceptor.DURATION_TIMER_NAME);
    assertThat(timer.count()).isEqualTo(1);

    for (final Meter meter : List.of(requestCount, responseCount, rpcCount, timer)) {
      for (final Tag tag : commonTags) {
        assertThat(meter.getId().getTag(tag.getKey())).isEqualTo(tag.getValue());
      }
    }

    assertThat(rpcCount.getId().getTag("statusCode")).isEqualTo("OK");
  }

  @Test
  void streaming() throws StatusException, InterruptedException, TimeoutException {
    final EchoServiceGrpc.EchoServiceBlockingV2Stub client = EchoServiceGrpc.newBlockingV2Stub(channel);
    final BlockingClientCall<EchoRequest, EchoResponse> echoStream = client.echoStream();
    echoStream.write(EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("1")).build());
    echoStream.write(EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("2")).build());
    echoStream.read();
    echoStream.read();
    echoStream.halfClose();

    // Make sure we don't check metrics before our close is processed
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);

    final Counter requestCount = find(Counter.class, MetricServerInterceptor.REQUEST_MESSAGE_COUNTER_NAME);
    assertThat(requestCount.count()).isCloseTo(2.0, offset(0.01));

    final Counter responseCount = find(Counter.class, MetricServerInterceptor.RESPONSE_COUNTER_NAME);
    assertThat(responseCount.count()).isCloseTo(2.0, offset(0.01));

    final Counter rpcCount = find(Counter.class, MetricServerInterceptor.RPC_COUNTER_NAME);
    assertThat(rpcCount.count()).isCloseTo(1.0, offset(0.01));

    final Timer timer = find(Timer.class, MetricServerInterceptor.DURATION_TIMER_NAME);
    assertThat(timer.count()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void clientRelease(boolean enabled) throws UnrecognizedUserAgentException {
    final UserAgent ua = UserAgentUtil.parseUserAgentString(USER_AGENT);
    when(clientReleaseManager.isVersionActive(ua.platform(), ua.version())).thenReturn(enabled);
    final EchoServiceGrpc.EchoServiceBlockingV2Stub client = EchoServiceGrpc.newBlockingV2Stub(channel);
    client.echo(EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("hello")).build());

    final String actualClientVersion = find(Meter.class, MetricServerInterceptor.REQUEST_MESSAGE_COUNTER_NAME)
        .getId()
        .getTag("clientVersion");
    final String expectedClientVersion = enabled ? ua.version().toString() : null;

    assertThat(expectedClientVersion).isEqualTo(actualClientVersion);
  }

  private <T extends Meter> T find(Class<T> cls, final String name) {
    final Meter meter = simpleMeterRegistry.getMeters().stream()
        .filter(m -> m.getId().getName().equals(name))
        .findFirst()
        .orElseThrow();
    if (cls.isInstance(meter)) {
      return cls.cast(meter);
    }
    throw new IllegalArgumentException("Meter " + name + " should be an instance of " + cls);
  }
}
