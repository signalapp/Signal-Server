/*
 * Copyright 2026 Signal Messenger, LLC
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
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.BlockingClientCall;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicGrpcCompressionConfiguration;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.tests.util.FakeDynamicConfigurationManager;

class MetricServerStreamTracerTest {

  private static final String USER_AGENT = "Signal-Android/12.53.7 (Android 14.1; libsignal)";

  private static final EchoRequest REQUEST = EchoRequest.newBuilder()
      .setPayload(ByteString.copyFromUtf8("compressible ".repeat(100)))
      .build();

  private Server server;
  private ManagedChannel channel;
  private SimpleMeterRegistry simpleMeterRegistry;

  @BeforeEach
  void setUp() throws Exception {
    simpleMeterRegistry = new SimpleMeterRegistry();

    final MockRequestAttributesInterceptor mockRequestAttributesInterceptor = new MockRequestAttributesInterceptor();
    mockRequestAttributesInterceptor.setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), USER_AGENT, null));

    // Only compress echo
    final DynamicConfiguration configuration = mock(DynamicConfiguration.class);
    when(configuration.getGrpcCompression())
        .thenReturn(new DynamicGrpcCompressionConfiguration(Set.of(), Set.of("org.signal.chat.rpc.EchoService/echo")));

    // By default, the in-process transport doesn't report message sizes to stream tracers at all, and it never
    // compresses messages; use a real transport instead
    server = NettyServerBuilder.forPort(0)
        .directExecutor()
        .addService(new EchoServiceImpl())
        .addStreamTracerFactory(new MetricServerStreamTracer.Factory())
        .intercept(new GrpcCompressionInterceptor(new FakeDynamicConfigurationManager<>(configuration)))
        .intercept(new MetricServerInterceptor(simpleMeterRegistry, mock(ClientReleaseManager.class)))
        .intercept(mockRequestAttributesInterceptor)
        .build()
        .start();

    channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
        .directExecutor()
        .usePlaintext()
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
  void uncompressed() {
    final EchoResponse response = EchoServiceGrpc.newBlockingStub(channel).echo2(REQUEST);

    final Counter wireBytes = find(MetricServerInterceptor.RESPONSE_BYTES_COUNTER_NAME, "echo2");
    assertThat(wireBytes.count()).isCloseTo(response.getSerializedSize(), offset(0.01));
  }

  @Test
  void streaming() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingV2Stub client = EchoServiceGrpc.newBlockingV2Stub(channel);
    final BlockingClientCall<EchoRequest, EchoResponse> echoStream = client.echoStream();

    final List<EchoRequest> requests = IntStream.range(0, 2)
        .mapToObj(i -> EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8(String.valueOf(i))).build())
        .toList();

    final List<EchoResponse> responses = new ArrayList<>(requests.size());

    for (final EchoRequest request : requests) {
      echoStream.write(request);
      responses.add(echoStream.read());
    }

    echoStream.halfClose();

    // Wait for the close to finish; metrics are reported after close
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);

    final int expectedBytes = responses.stream().mapToInt(EchoResponse::getSerializedSize).sum();

    assertThat(find(MetricServerInterceptor.RESPONSE_BYTES_COUNTER_NAME, "echoStream").count())
        .isCloseTo(expectedBytes, offset(0.01));
  }

  @Test
  void compressed() {
    final EchoResponse response = EchoServiceGrpc.newBlockingStub(channel).echo(REQUEST);

    final Counter wireBytes = find(MetricServerInterceptor.RESPONSE_BYTES_COUNTER_NAME, "echo");

    assertThat(wireBytes.count()).isPositive().isLessThan(response.getSerializedSize());

    // we expect >90% compression
    assertThat(wireBytes.count() / response.getSerializedSize()).isLessThan(0.1);

    final Tags expectedTags = Tags.of(
        "platform", "android",
        "grpcService", "org.signal.chat.rpc.EchoService",
        "method", "echo");

    for (final Tag tag : expectedTags) {
      assertThat(wireBytes.getId().getTag(tag.getKey())).isEqualTo(tag.getValue());
    }
  }

  private Counter find(final String name, final String method) {
    return simpleMeterRegistry.find(name).tag("method", method).counter();
  }
}
