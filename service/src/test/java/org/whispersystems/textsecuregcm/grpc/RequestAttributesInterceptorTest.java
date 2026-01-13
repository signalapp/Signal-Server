/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.rpc.GetRequestAttributesRequest;
import org.signal.chat.rpc.GetRequestAttributesResponse;
import org.signal.chat.rpc.RequestAttributesGrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestAttributesInterceptorTest {

  private static final String USER_AGENT = "Signal-Android/4.53.7 (Android 8.1; libsignal)";
  private Server server;
  private AtomicBoolean removeUserAgent;

  @BeforeEach
  void setUp() throws Exception {
    removeUserAgent = new AtomicBoolean(false);

    server = NettyServerBuilder.forPort(0)
        .directExecutor()
        .intercept(new RequestAttributesInterceptor())
        // the grpc client always inserts a user-agent if we don't set one, so to test missing UAs we remove the header
        // on the server-side
        .intercept(new ServerInterceptor() {
          @Override
          public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
              final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {
            if (removeUserAgent.get()) {
              headers.removeAll(Metadata.Key.of("user-agent", Metadata.ASCII_STRING_MARSHALLER));
            }
            return next.startCall(call, headers);
          }
        })
        .addService(new RequestAttributesServiceImpl())
        .build()
        .start();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.shutdownNow();
    server.awaitTermination(1, TimeUnit.SECONDS);
  }

  private static List<Arguments> handleInvalidAcceptLanguage() {
    return List.of(
        Arguments.argumentSet("Null Accept-Language header", Optional.empty()),
        Arguments.argumentSet("Empty Accept-Language header", Optional.of("")),
        Arguments.argumentSet("Invalid Accept-Language header", Optional.of("This is not a valid language preference list")));
  }

  @ParameterizedTest
  @MethodSource
  void handleInvalidAcceptLanguage(Optional<String> acceptLanguageHeader) throws Exception {
    final Metadata metadata = new Metadata();
    acceptLanguageHeader.ifPresent(h -> metadata
        .put(Metadata.Key.of("accept-language", Metadata.ASCII_STRING_MARSHALLER), h));
    final GetRequestAttributesResponse response = getRequestAttributes(metadata);
    assertEquals(response.getAcceptableLanguagesCount(), 0);
  }

  @Test
  void handleMissingUserAgent() throws InterruptedException {
    removeUserAgent.set(true);
    final GetRequestAttributesResponse response = getRequestAttributes(new Metadata());
    assertEquals("", response.getUserAgent());
  }

  @Test
  void allAttributes() throws InterruptedException {
    final Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("accept-language", Metadata.ASCII_STRING_MARSHALLER), "ja,en;q=0.4");
    metadata.put(Metadata.Key.of("x-forwarded-for", Metadata.ASCII_STRING_MARSHALLER), "127.0.0.3");
    final GetRequestAttributesResponse response = getRequestAttributes(metadata);

    assertTrue(response.getUserAgent().contains(USER_AGENT));
    assertEquals("127.0.0.3", response.getRemoteAddress());
    assertEquals(2, response.getAcceptableLanguagesCount());
    assertEquals("ja", response.getAcceptableLanguages(0));
    assertEquals("en;q=0.4", response.getAcceptableLanguages(1));
  }

  @Test
  void useSocketAddrIfHeaderMissing() throws InterruptedException {
    final GetRequestAttributesResponse response = getRequestAttributes(new Metadata());
    assertEquals("127.0.0.1", response.getRemoteAddress());
  }

  private GetRequestAttributesResponse getRequestAttributes(Metadata metadata)
      throws InterruptedException {
    final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
        .directExecutor()
        .usePlaintext()
        .userAgent(USER_AGENT)
        .build();
    try {
      final RequestAttributesGrpc.RequestAttributesBlockingStub client = RequestAttributesGrpc
          .newBlockingStub(channel)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
      return client.getRequestAttributes(GetRequestAttributesRequest.getDefaultInstance());
    } finally {
      channel.shutdownNow();
      channel.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

}
