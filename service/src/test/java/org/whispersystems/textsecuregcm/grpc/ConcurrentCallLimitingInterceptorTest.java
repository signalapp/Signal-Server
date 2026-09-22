/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;

class ConcurrentCallLimitingInterceptorTest {

  private static final String SERVER_NAME = "ConcurrentCallLimitingInterceptorTest";
  private Server server;
  private ManagedChannel channel;

  private static final int MAX_CONCURRENT_CALLS = 10;
  private final CountDownLatch handlerStarted = new CountDownLatch(MAX_CONCURRENT_CALLS);
  private final CountDownLatch handlerReleased = new CountDownLatch(1);

  private final class BlockingEchoService extends EchoServiceGrpc.EchoServiceImplBase {

    @Override
    public void echo(final EchoRequest request, final StreamObserver<EchoResponse> responseObserver) {
      handlerStarted.countDown();
      try {
        handlerReleased.await();
      } catch (final InterruptedException e) {
        fail(e);
      }
      responseObserver.onNext(EchoResponse.newBuilder().setPayload(request.getPayload()).build());
      responseObserver.onCompleted();
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    handlerReleased.countDown();
    server.shutdownNow();
    channel.shutdownNow();
    server.awaitTermination(1, TimeUnit.SECONDS);
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void maxConcurrentCallsReached() throws Exception {
    final ConcurrentCallLimitingInterceptor interceptor = new ConcurrentCallLimitingInterceptor(MAX_CONCURRENT_CALLS);
    server = InProcessServerBuilder.forName(SERVER_NAME)
        .addService(new BlockingEchoService())
        .intercept(interceptor)
        .build()
        .start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).build();
    final ByteString payload = ByteString.copyFromUtf8("payload");

    final EchoRequest request = EchoRequest.newBuilder().setPayload(payload).build();

    // Initiate MAX_CONCURRENT_CALLS (non-blocking) calls that will wait until we signal them to finish
    final List<CompletableFuture<EchoResponse>> responses = IntStream.range(0, MAX_CONCURRENT_CALLS)
        .mapToObj(_ -> {
          final CompletableFuture<EchoResponse> responseFuture = new CompletableFuture<>();
          EchoServiceGrpc.newStub(channel).echo(request, new StreamObserver<>() {
            @Override
            public void onNext(final EchoResponse value) {
              responseFuture.complete(value);
            }

            @Override
            public void onError(final Throwable t) {
              responseFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
          });
          return responseFuture;
        })
        .toList();

    assertThat(handlerStarted.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(interceptor.getActiveCalls()).isEqualTo(MAX_CONCURRENT_CALLS);

    // Finally, issue a blocking call that should fail because all slots are occupied
    final EchoServiceGrpc.EchoServiceBlockingStub blockingStub =
        EchoServiceGrpc.newBlockingStub(channel).withDeadlineAfter(1, TimeUnit.SECONDS);

    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE, () -> blockingStub.echo(request));

    handlerReleased.countDown();
    // Verify that the other requests completed normally
    CompletableFuture.allOf(responses.toArray(new CompletableFuture[responses.size()])).get(1, TimeUnit.SECONDS);
    responses.forEach(response -> assertThat(response.join().getPayload()).isEqualTo(payload));
  }

  @Test
  void releasesSlotWhenHandlerThrows() throws Exception {
    final ConcurrentCallLimitingInterceptor interceptor = new ConcurrentCallLimitingInterceptor(1);

    server = InProcessServerBuilder.forName(SERVER_NAME)
        .addService(new EchoServiceGrpc.EchoServiceImplBase() {
          @Override
          public void echo(final EchoRequest request, final StreamObserver<EchoResponse> responseObserver) {
            throw new RuntimeException("handler failed!");
          }
        })
        .intercept(interceptor)
        .build()
        .start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).build();

    final EchoServiceGrpc.EchoServiceBlockingStub blockingStub = EchoServiceGrpc.newBlockingStub(channel);
    GrpcTestUtils.assertStatusException(Status.UNKNOWN, () -> blockingStub.echo(EchoRequest.newBuilder().build()));

    // the gRPC response can arrive before onComplete() because it is called asynchronously, so poll until there are no active calls.
    final Instant deadline = Clock.systemUTC().instant().plusSeconds(1);
    while (interceptor.getActiveCalls() > 0 && Clock.systemUTC().instant().isBefore(deadline)) {
      Thread.sleep(Duration.ofMillis(10));
    }
    assertThat(interceptor.getActiveCalls()).isZero();
  }

  @Test
  void releasesSlotWhenCallCancelled() throws Exception {
    final ConcurrentCallLimitingInterceptor interceptor = new ConcurrentCallLimitingInterceptor(1);
    final CountDownLatch latch = new CountDownLatch(1);

    server = InProcessServerBuilder.forName(SERVER_NAME)
        .addService(new EchoServiceGrpc.EchoServiceImplBase() {
          @Override
          public void echo(final EchoRequest request, final StreamObserver<EchoResponse> responseObserver) {
            // Never complete the response; the call stays in-flight until the client cancels.
            latch.countDown();
          }
        })
        .intercept(interceptor)
        .build()
        .start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).build();

    //noinspection resource
    final Context.CancellableContext cancellableContext = Context.current().withCancellation();
    cancellableContext.run(() -> EchoServiceGrpc.newStub(channel).echo(EchoRequest.newBuilder().build(),
        new StreamObserver<>() {
          @Override
          public void onNext(final EchoResponse value) {
          }

          @Override
          public void onError(final Throwable t) {
          }

          @Override
          public void onCompleted() {
          }
        }));

    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(interceptor.getActiveCalls()).isEqualTo(1);

    assertThat(cancellableContext.cancel(new RuntimeException("client cancelled"))).isTrue();

    // the gRPC response can arrive before onComplete() because it is called asynchronously, so poll until there are no active calls.
    final Instant deadline = Clock.systemUTC().instant().plusSeconds(1);
    while (interceptor.getActiveCalls() > 0 && Clock.systemUTC().instant().isBefore(deadline)) {
      Thread.sleep(Duration.ofMillis(10));
    }
    assertThat(interceptor.getActiveCalls()).isZero();
  }
}
