/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.protobuf.ByteString;
import com.vdurmont.semver4j.Semver;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.grpc.EchoServiceImpl;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class UserAgentInterceptorTest {

  @ParameterizedTest
  @MethodSource
  void testInterceptor(final String header, final ClientPlatform platform, final String version) throws Exception {

    final AtomicReference<UserAgent> observedUserAgent = new AtomicReference<>(null);
    final EchoServiceImpl serviceImpl = new EchoServiceImpl() {
        @Override
        public void echo(EchoRequest req, StreamObserver<EchoResponse> responseObserver) {
          observedUserAgent.set(UserAgentUtil.userAgentFromGrpcContext());
          super.echo(req, responseObserver);
        }
      };

    final Server testServer = InProcessServerBuilder.forName("RemoteDeprecationFilterTest")
        .directExecutor()
        .addService(serviceImpl)
        .intercept(new UserAgentInterceptor())
        .build()
        .start();

    try {
      final ManagedChannel channel = InProcessChannelBuilder.forName("RemoteDeprecationFilterTest")
          .directExecutor()
          .userAgent(header)
          .build();

      final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);

      final EchoRequest req = EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8("cluck cluck, i'm a parrot")).build();
      assertEquals("cluck cluck, i'm a parrot", client.echo(req).getPayload().toStringUtf8());
      if (platform == null) {
        assertNull(observedUserAgent.get());
      } else {
        assertEquals(platform, observedUserAgent.get().getPlatform());
        assertEquals(new Semver(version), observedUserAgent.get().getVersion());
        // can't assert on the additional specifiers because they include internal details of the grpc in-process channel itself
      }
    } finally {
      testServer.shutdownNow();
      testServer.awaitTermination();
    }
  }

  private static Stream<Arguments> testInterceptor() {
    return Stream.of(
        Arguments.of(null, null, null),
        Arguments.of("", null, null),
        Arguments.of("Unrecognized UA", null, null),
        Arguments.of("Signal-Android/4.68.3", ClientPlatform.ANDROID, "4.68.3"),
        Arguments.of("Signal-iOS/3.9.0", ClientPlatform.IOS, "3.9.0"),
        Arguments.of("Signal-Desktop/1.2.3", ClientPlatform.DESKTOP, "1.2.3"),
        Arguments.of("Signal-Desktop/8.0.0-beta.2", ClientPlatform.DESKTOP, "8.0.0-beta.2"),
        Arguments.of("Signal-iOS/8.0.0-beta.2", ClientPlatform.IOS, "8.0.0-beta.2"));
  }
}
