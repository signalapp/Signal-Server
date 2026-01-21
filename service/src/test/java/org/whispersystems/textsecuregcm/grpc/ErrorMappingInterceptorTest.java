/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.signal.chat.rpc.ReactorEchoServiceGrpc;
import org.signal.chat.rpc.SimpleEchoServiceGrpc;
import reactor.core.publisher.Mono;

class ErrorMappingInterceptorTest {

  private Server server;
  private ManagedChannel channel;


  @BeforeEach
  void setUp() throws Exception {
    channel = InProcessChannelBuilder.forName("ErrorMappingInterceptorTest")
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
  public void includeDetailsSimpleGrpc() throws Exception {
    final StatusRuntimeException e = StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.INVALID_ARGUMENT.value())
        .addDetails(Any.pack(ErrorInfo.newBuilder()
            .setDomain("test")
            .setReason("TEST")
            .build()))
        .build());

    server = InProcessServerBuilder.forName("ErrorMappingInterceptorTest")
        .directExecutor()
        .addService(new SimpleEchoServiceErrorImpl(e))
        .intercept(new ErrorMappingInterceptor())
        .build()
        .start();

    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, "TEST", () ->
        client.echo(EchoRequest.getDefaultInstance()));
  }

  @Test
  public void includeDetailsReactiveGrpc() throws Exception {
    final StatusRuntimeException e = StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
        .setCode(Status.Code.INVALID_ARGUMENT.value())
        .addDetails(Any.pack(ErrorInfo.newBuilder()
            .setDomain("test")
            .setReason("TEST")
            .build()))
        .build());

    server = InProcessServerBuilder.forName("ErrorMappingInterceptorTest")
        .directExecutor()
        .addService(new ReactorEchoServiceErrorImpl(e))
        .intercept(new ErrorMappingInterceptor())
        .build()
        .start();

    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, "TEST", () ->
        client.echo(EchoRequest.getDefaultInstance()));
  }


  @Test
  public void mapIOExceptionsReactive() throws Exception {
    server = InProcessServerBuilder.forName("ErrorMappingInterceptorTest")
        .directExecutor()
        .addService(new ReactorEchoServiceErrorImpl(new IOException("test")))
        .intercept(new ErrorMappingInterceptor())
        .build()
        .start();

    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, "UNAVAILABLE", () ->
        client.echo(EchoRequest.getDefaultInstance()));
  }

  @Test
  public void mapIOExceptionsSimple() throws Exception {
    server = InProcessServerBuilder.forName("ErrorMappingInterceptorTest")
        .directExecutor()
        .addService(new SimpleEchoServiceErrorImpl(new UncheckedIOException(new IOException("test"))))
        .intercept(new ErrorMappingInterceptor())
        .build()
        .start();

    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, "UNAVAILABLE", () ->
        client.echo(EchoRequest.getDefaultInstance()));
  }

  @Test
  public void mapWrappedIOExceptionsSimple() throws Exception {
    server = InProcessServerBuilder.forName("ErrorMappingInterceptorTest")
        .directExecutor()
        .addService(new SimpleEchoServiceErrorImpl(new CompletionException(new UncheckedIOException(new IOException("test")))))
        .intercept(new ErrorMappingInterceptor())
        .build()
        .start();

    final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);
    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, "UNAVAILABLE", () ->
        client.echo(EchoRequest.getDefaultInstance()));
  }


  static class ReactorEchoServiceErrorImpl extends ReactorEchoServiceGrpc.EchoServiceImplBase {

    private final Exception exception;

    ReactorEchoServiceErrorImpl(final Exception exception) {
      this.exception = exception;
    }

    @Override
    public Mono<EchoResponse> echo(final EchoRequest echoRequest) {
      return Mono.error(exception);
    }

    @Override
    public Throwable onErrorMap(Throwable throwable) {
      return new IllegalArgumentException(throwable);
    }
  }

  static class SimpleEchoServiceErrorImpl extends SimpleEchoServiceGrpc.EchoServiceImplBase {

    private final RuntimeException exception;

    SimpleEchoServiceErrorImpl(final RuntimeException exception) {
      this.exception = exception;
    }

    @Override
    public EchoResponse echo(final EchoRequest echoRequest) {
      throw exception;
    }

  }
}
