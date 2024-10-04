package org.whispersystems.textsecuregcm.keytransparency;

import com.google.protobuf.AbstractMessageLite;
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
import org.signal.keytransparency.client.ConsistencyParameters;
import org.signal.keytransparency.client.KeyTransparencyQueryServiceGrpc;
import org.signal.keytransparency.client.MonitorKey;
import org.signal.keytransparency.client.MonitorRequest;
import org.signal.keytransparency.client.SearchRequest;
import org.whispersystems.textsecuregcm.util.CompletableFutureUtil;

public class KeyTransparencyServiceClient implements Managed {

  private final Executor callbackExecutor;
  private final String host;
  private final int port;
  private final ChannelCredentials tlsChannelCredentials;
  private ManagedChannel channel;
  private KeyTransparencyQueryServiceGrpc.KeyTransparencyQueryServiceFutureStub stub;

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

  public CompletableFuture<byte[]> search(
      final ByteString searchKey,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize,
      final Duration timeout) {
    final SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder()
        .setSearchKey(searchKey);

    final ConsistencyParameters.Builder consistency = ConsistencyParameters.newBuilder();
    lastTreeHeadSize.ifPresent(consistency::setLast);
    distinguishedTreeHeadSize.ifPresent(consistency::setDistinguished);

    searchRequestBuilder.setConsistency(consistency);

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .search(searchRequestBuilder.build()), callbackExecutor)
        .thenApply(AbstractMessageLite::toByteArray);
  }

  public CompletableFuture<byte[]> monitor(final List<MonitorKey> monitorKeys,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize,
      final Duration timeout) {
    final MonitorRequest.Builder monitorRequestBuilder = MonitorRequest.newBuilder()
        .addAllContactKeys(monitorKeys);

    final ConsistencyParameters.Builder consistency = ConsistencyParameters.newBuilder();
    lastTreeHeadSize.ifPresent(consistency::setLast);
    distinguishedTreeHeadSize.ifPresent(consistency::setDistinguished);

    monitorRequestBuilder.setConsistency(consistency);

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .monitor(monitorRequestBuilder.build()), callbackExecutor)
        .thenApply(AbstractMessageLite::toByteArray);
  }

  private static Deadline toDeadline(final Duration timeout) {
    return Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void start() throws Exception {
    channel = Grpc.newChannelBuilderForAddress(host, port, tlsChannelCredentials)
        .idleTimeout(1, TimeUnit.MINUTES)
        .build();
    stub = KeyTransparencyQueryServiceGrpc.newFutureStub(channel);
  }

  @Override
  public void stop() throws Exception {
    if (channel != null) {
      channel.shutdown();
    }
  }
}
