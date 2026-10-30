/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Codec;
import io.grpc.DecompressorRegistry;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicGrpcCompressionConfiguration;
import org.whispersystems.textsecuregcm.tests.util.FakeDynamicConfigurationManager;

class GrpcCompressionInterceptorTest {

  private static final String SERVER_NAME = GrpcCompressionInterceptor.class.getSimpleName();

  private static final Metadata.Key<String> GRPC_ENCODING_KEY =
      Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

  private static final EchoRequest REQUEST = EchoRequest.newBuilder()
      .setPayload(ByteString.copyFromUtf8("compressible ".repeat(100)))
      .build();

  private Server server;
  private ManagedChannel channel;
  private final ResponseHeaderCapturingInterceptor responseHeaderCapturingInterceptor =
      new ResponseHeaderCapturingInterceptor();

  @AfterEach
  void tearDown() throws Exception {
    server.shutdownNow();
    channel.shutdownNow();
    server.awaitTermination(1, TimeUnit.SECONDS);
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void noCompressionByDefault() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(Collections.emptySet(), Collections.emptySet(), DecompressorRegistry.getDefaultInstance());

    assertThat(client.echo(REQUEST).getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("identity");
  }

  @Test
  void compressByMethod() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(Collections.emptySet(), Set.of("org.signal.chat.rpc.EchoService/echo"),
            DecompressorRegistry.getDefaultInstance());

    final EchoResponse echo = client.echo(REQUEST);
    assertThat(echo.getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("gzip");

    final EchoResponse echo2 = client.echo2(REQUEST);
    assertThat(echo2.getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("identity");
  }

  @Test
  void compressByService() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(Set.of("org.signal.chat.rpc.EchoService"), Collections.emptySet(),
            DecompressorRegistry.getDefaultInstance());

    final EchoResponse echo = client.echo(REQUEST);
    assertThat(echo.getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("gzip");

    final EchoResponse echo2 = client.echo2(REQUEST);
    assertThat(echo2.getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("gzip");
  }

  @Test
  void compressByServiceWrongService() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(Set.of("org.signal.chat.rpc.NotEchoService"), Collections.emptySet(),
            DecompressorRegistry.getDefaultInstance());

    assertThat(client.echo(REQUEST).getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("identity");
  }

  @Test
  void clientDoesNotAcceptGzip() throws Exception {
    final EchoServiceGrpc.EchoServiceBlockingStub client =
        setup(Set.of("org.signal.chat.rpc.EchoService"), Collections.emptySet(),
            DecompressorRegistry.emptyInstance().with(Codec.Identity.NONE, true));

    assertThat(client.echo(REQUEST).getPayload()).isEqualTo(REQUEST.getPayload());
    assertThat(responseHeaderCapturingInterceptor.responseEncoding).isEqualTo("identity");
  }

  private EchoServiceGrpc.EchoServiceBlockingStub setup(
      final Set<String> compressedServices,
      final Set<String> compressedMethods,
      final DecompressorRegistry clientDecompressorRegistry) throws IOException {

    final DynamicConfiguration configuration = mock(DynamicConfiguration.class);
    when(configuration.getGrpcCompression())
        .thenReturn(new DynamicGrpcCompressionConfiguration(compressedServices, compressedMethods));

    server = InProcessServerBuilder.forName(SERVER_NAME)
        .directExecutor()
        .addService(new EchoServiceImpl())
        .intercept(new GrpcCompressionInterceptor(new FakeDynamicConfigurationManager<>(configuration)))
        .build()
        .start();

    channel = InProcessChannelBuilder.forName(SERVER_NAME)
        .directExecutor()
        .decompressorRegistry(clientDecompressorRegistry)
        .intercept(responseHeaderCapturingInterceptor)
        .build();

    return EchoServiceGrpc.newBlockingStub(channel);
  }

  private static class ResponseHeaderCapturingInterceptor implements ClientInterceptor {

    @Nullable
    private volatile String responseEncoding;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {

      return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          responseEncoding = null;

          super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
            @Override
            public void onHeaders(final Metadata responseHeaders) {
              responseEncoding = responseHeaders.get(GRPC_ENCODING_KEY);
              super.onHeaders(responseHeaders);
            }
          }, headers);
        }
      };
    }
  }
}
