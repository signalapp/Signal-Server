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
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.rpc.ErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.BlockingClientCall;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.signal.chat.rpc.SimpleTagTestServiceGrpc;
import org.signal.chat.rpc.TagResponse;
import org.signal.chat.rpc.TagTestServiceGrpc;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class MetricServerInterceptorTest {

  private static final String USER_AGENT = "Signal-Android/4.53.7 (Android 8.1; libsignal)";

  private Server server;
  private ManagedChannel channel;
  private SimpleMeterRegistry simpleMeterRegistry;
  private ClientReleaseManager clientReleaseManager;

  private Supplier<TagResponse> tagResponseSupplier;

  @BeforeEach
  void setUp() throws Exception {
    simpleMeterRegistry = new SimpleMeterRegistry();
    clientReleaseManager = mock(ClientReleaseManager.class);
    tagResponseSupplier = mock(Supplier.class);
    final MockRequestAttributesInterceptor mockRequestAttributesInterceptor = new MockRequestAttributesInterceptor();
    mockRequestAttributesInterceptor.setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), USER_AGENT, null));

    server = InProcessServerBuilder.forName("MetricServerInterceptorTest")
        .directExecutor()
        .addService(new EchoServiceImpl())
        .addService(new TagTestServiceImpl(tagResponseSupplier))
        .intercept(new MetricServerInterceptor(simpleMeterRegistry, clientReleaseManager))
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
  void streaming() throws StatusException, InterruptedException {
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

  static Stream<Arguments> testUnaryOkResponseReason() {
    return Stream.of(
            Arguments.argumentSet("Default reason", TagResponse.newBuilder().build(), "success"),
            Arguments.argumentSet("No reason", TagResponse.newBuilder().setNoReason(true).build(), "success"),
            Arguments.argumentSet("Explicitly set reason", TagResponse.newBuilder().setReason1(true).build(), "reason_1"),
            Arguments.argumentSet("Nested reason", TagResponse.newBuilder().setNestedReason(TagResponse.NestedReason.newBuilder().setReason(true)).build(), "nested_reason"));
  }

  @ParameterizedTest
  @MethodSource
  void testUnaryOkResponseReason(TagResponse response, String expectedReason) throws InterruptedException {
    final TagTestServiceGrpc.TagTestServiceBlockingStub tagTestServiceBlockingStub =
        TagTestServiceGrpc.newBlockingStub(channel);
    when(tagResponseSupplier.get()).thenReturn(response);
    tagTestServiceBlockingStub.tagEndpoint(Empty.getDefaultInstance());

    final Counter rpcCount = find(Counter.class, MetricServerInterceptor.RPC_COUNTER_NAME);
    assertThat(rpcCount.count()).isCloseTo(1.0, offset(0.01));
    assertThat(rpcCount.getId().getTag("statusCode")).isEqualTo("OK");
    assertThat(rpcCount.getId().getTag("reason")).isEqualTo(expectedReason);
  }

  @Test
  public void testConflictingReasons() {
    final TagTestServiceGrpc.TagTestServiceBlockingStub tagTestServiceBlockingStub =
        TagTestServiceGrpc.newBlockingStub(channel);
    when(tagResponseSupplier.get())
        .thenReturn(TagResponse.newBuilder().setReason1(true).setConflictingReason(true).build());
    tagTestServiceBlockingStub.tagEndpoint(Empty.getDefaultInstance());

    // We make no promises if proto fields that have reason tags are present on a message, but this tests for the sane
    // behavior that at least one of these tags makes it into the metric.
    assertThat(find(Counter.class, MetricServerInterceptor.RPC_COUNTER_NAME).getId().getTag("reason"))
        .isIn("duplicate_reason", "reason_1");
  }

  @CartesianTest
  public void testStatusErrorResponseReason(
      @CartesianTest.Enum(mode = CartesianTest.Enum.Mode.EXCLUDE, names = {"OK"}) Status.Code statusCode,
      @CartesianTest.Values(strings = {"test", "", "null"}) String reasonParam) {

    final String reason, expectedReasonTag;
    if (reasonParam.equals("null")) {
      reason = null;
      expectedReasonTag = MetricServerInterceptor.DEFAULT_ERROR_REASON;
    } else {
      reason = reasonParam;
      expectedReasonTag = reasonParam;
    }

    final TagTestServiceGrpc.TagTestServiceBlockingStub tagTestServiceBlockingStub =
        TagTestServiceGrpc.newBlockingStub(channel);

    final com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder()
        .setCode(statusCode.value())
        .setMessage("test");
    if (reason != null) {
      builder.addDetails(Any.pack(ErrorInfo.newBuilder()
          .setDomain("domain")
          .setReason(reason)
          .build()));
    }

    when(tagResponseSupplier.get()).thenThrow(StatusProto.toStatusRuntimeException(builder.build()));

    GrpcTestUtils.assertStatusException(statusCode.toStatus(),
        () -> tagTestServiceBlockingStub.tagEndpoint(Empty.getDefaultInstance()));

    final Counter rpcCount = find(Counter.class, MetricServerInterceptor.RPC_COUNTER_NAME);
    assertThat(rpcCount.count()).isCloseTo(1.0, offset(0.01));
    assertThat(rpcCount.getId().getTag("statusCode")).isEqualTo(statusCode.name());
    assertThat(rpcCount.getId().getTag("reason")).isEqualTo(expectedReasonTag);
  }

  @Test
  public void testStreamingResponseReason() {
    final TagTestServiceGrpc.TagTestServiceBlockingStub tagTestServiceBlockingStub =
        TagTestServiceGrpc.newBlockingStub(channel);
    when(tagResponseSupplier.get())
        .thenReturn(TagResponse.newBuilder().setReason1(true).build())
        .thenReturn(TagResponse.newBuilder().setNoReason(true).build())
        .thenReturn(null);

    tagTestServiceBlockingStub.streamingTagEndpoint(Empty.getDefaultInstance()).forEachRemaining(_ -> {});
    final Counter messageCounter = find(Counter.class, MetricServerInterceptor.RESPONSE_COUNTER_NAME);
    assertThat(messageCounter.count()).isCloseTo(2.0, offset(0.01));

    final Counter rpcCount = find(Counter.class, MetricServerInterceptor.RPC_COUNTER_NAME);
    assertThat(rpcCount.count()).isCloseTo(1.0, offset(0.01));
    assertThat(rpcCount.getId().getTag("statusCode")).isEqualTo("OK");
    assertThat(rpcCount.getId().getTag("reason")).isEqualTo(MetricServerInterceptor.DEFAULT_SUCCESS_REASON);
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

  class TagTestServiceImpl extends SimpleTagTestServiceGrpc.TagTestServiceImplBase {

    private Supplier<TagResponse> tagResponseSupplier;
    TagTestServiceImpl(Supplier<TagResponse> tagResponseSupplier) {
      this.tagResponseSupplier = tagResponseSupplier;
    }

    @Override
    public TagResponse tagEndpoint(final Empty request) {
      return tagResponseSupplier.get();
    }

    @Override
    public Flow.Publisher<TagResponse> streamingTagEndpoint(com.google.protobuf.Empty request) {
      return JdkFlowAdapter.publisherToFlowPublisher(Flux.<TagResponse>create(sink -> {
            while (!sink.isCancelled()) {
              TagResponse item = tagResponseSupplier.get();
              if (item == null) {
                sink.complete();
                break;
              }
              sink.next(item);
            }
          })
          .subscribeOn(Schedulers.boundedElastic()));
    }
  }
}
