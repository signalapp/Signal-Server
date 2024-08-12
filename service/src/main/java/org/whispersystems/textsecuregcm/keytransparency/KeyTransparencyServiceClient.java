package org.whispersystems.textsecuregcm.keytransparency;

import com.google.protobuf.ByteString;
import io.dropwizard.lifecycle.Managed;
import io.grpc.ChannelCredentials;
import io.grpc.Deadline;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.grpc.TlsChannelCredentials;
import katie.KatieGrpc;
import katie.MonitorKey;
import katie.MonitorRequest;
import katie.MonitorResponse;
import katie.SearchRequest;
import katie.SearchResponse;
import org.whispersystems.textsecuregcm.util.CompletableFutureUtil;

public class KeyTransparencyServiceClient implements Managed {

  private final Executor callbackExecutor;
  private final String host;
  private final int port;
  private final ChannelCredentials tlsChannelCredentials;
  private ManagedChannel channel;
  private KatieGrpc.KatieFutureStub stub;

  public KeyTransparencyServiceClient(
      final String host,
      final int port,
      final String tlsCertificate,
      final Executor callbackExecutor
  ) throws IOException {
    this.host = host;
    this.port = port;
    try (final ByteArrayInputStream certificateInputStream = new ByteArrayInputStream(
        tlsCertificate.getBytes(StandardCharsets.UTF_8))) {
      tlsChannelCredentials = TlsChannelCredentials.newBuilder()
          .trustManager(certificateInputStream)
          .build();
    }
    this.callbackExecutor = callbackExecutor;
  }

  public CompletableFuture<SearchResponse> search(
      final ByteString searchKey,
      final Optional<Long> lastTreeHeadSize,
      final Duration timeout) {
    final SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder()
        .setSearchKey(searchKey);

    lastTreeHeadSize.ifPresent(searchRequestBuilder::setLast);

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .search(searchRequestBuilder.build()), callbackExecutor);
  }

  public CompletableFuture<MonitorResponse> monitor(final List<MonitorKey> monitorKeys,
      final Optional<Long> lastTreeHeadSize,
      final Duration timeout) {
    final MonitorRequest.Builder monitorRequestBuilder = MonitorRequest.newBuilder()
        .addAllContactKeys(monitorKeys);

    lastTreeHeadSize.ifPresent(monitorRequestBuilder::setLast);

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .monitor(monitorRequestBuilder.build()), callbackExecutor);
  }

  private static Deadline toDeadline(final Duration timeout) {
    return Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void start() throws Exception {
    channel = Grpc.newChannelBuilderForAddress(host, port, tlsChannelCredentials)
        .idleTimeout(1, TimeUnit.MINUTES)
        .build();
    stub = KatieGrpc.newFutureStub(channel);
  }

  @Override
  public void stop() throws Exception {
    if (channel != null) {
      channel.shutdown();
    }
  }
}
