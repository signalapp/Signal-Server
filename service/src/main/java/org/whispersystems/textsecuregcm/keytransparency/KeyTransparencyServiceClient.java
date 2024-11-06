package org.whispersystems.textsecuregcm.keytransparency;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import io.dropwizard.lifecycle.Managed;
import io.grpc.ChannelCredentials;
import io.grpc.Deadline;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import io.micrometer.core.instrument.Metrics;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.signal.keytransparency.client.AciMonitorRequest;
import org.signal.keytransparency.client.ConsistencyParameters;
import org.signal.keytransparency.client.DistinguishedRequest;
import org.signal.keytransparency.client.E164MonitorRequest;
import org.signal.keytransparency.client.E164SearchRequest;
import org.signal.keytransparency.client.KeyTransparencyQueryServiceGrpc;
import org.signal.keytransparency.client.MonitorRequest;
import org.signal.keytransparency.client.SearchRequest;
import org.signal.keytransparency.client.UsernameHashMonitorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.CompletableFutureUtil;

public class KeyTransparencyServiceClient implements Managed {

  private static final String DAYS_UNTIL_CLIENT_CERTIFICATE_EXPIRATION_GAUGE_NAME =
      MetricsUtil.name(KeyTransparencyServiceClient.class, "daysUntilClientCertificateExpiration");

  private static final Logger logger = LoggerFactory.getLogger(KeyTransparencyServiceClient.class);

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
      final String clientCertificate,
      final String clientPrivateKey,
      final Executor callbackExecutor
  ) throws IOException {
    this.host = host;
    this.port = port;
    try (final ByteArrayInputStream certificateInputStream = new ByteArrayInputStream(
        tlsCertificate.getBytes(StandardCharsets.UTF_8));
        final ByteArrayInputStream clientCertificateInputStream = new ByteArrayInputStream(
            clientCertificate.getBytes(StandardCharsets.UTF_8));
        final ByteArrayInputStream clientPrivateKeyInputStream = new ByteArrayInputStream(
            clientPrivateKey.getBytes(StandardCharsets.UTF_8))
    ) {
      tlsChannelCredentials = TlsChannelCredentials.newBuilder()
          .trustManager(certificateInputStream)
          .keyManager(clientCertificateInputStream, clientPrivateKeyInputStream)
          .build();

      configureClientCertificateMetrics(clientCertificate);

    }
    this.callbackExecutor = callbackExecutor;
  }

  private void configureClientCertificateMetrics(String clientCertificate) {
    try {
      final CertificateFactory cf = CertificateFactory.getInstance("X.509");
      final Collection<? extends Certificate> certificates = cf.generateCertificates(
          new ByteArrayInputStream(clientCertificate.getBytes(StandardCharsets.UTF_8)));

      if (certificates.isEmpty()) {
        logger.warn("No client certificate found");
        return;
      }

      if (certificates.size() > 1) {
        throw new IllegalArgumentException("Unexpected number of client certificates: " + certificates.size());
      }

      final Certificate certificate = certificates.iterator().next();

      if (certificate instanceof X509Certificate x509Cert) {
        final Instant expiration = Instant.ofEpochMilli(x509Cert.getNotAfter().getTime());

        Metrics.gauge(DAYS_UNTIL_CLIENT_CERTIFICATE_EXPIRATION_GAUGE_NAME,
            this,
            (ignored) -> Duration.between(Instant.now(), expiration).toDays());

      } else {
        logger.error("Certificate was of unexpected type: {}", certificate.getClass().getName());
      }

    } catch (CertificateException e) {
      throw new AssertionError("JDKs are required to support X.509 algorithms", e);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public CompletableFuture<byte[]> search(
      final ByteString aci,
      final ByteString aciIdentityKey,
      final Optional<ByteString> usernameHash,
      final Optional<E164SearchRequest> e164SearchRequest,
      final Optional<Long> lastTreeHeadSize,
      final long distinguishedTreeHeadSize,
      final Duration timeout) {
    final SearchRequest.Builder searchRequestBuilder = SearchRequest.newBuilder()
        .setAci(aci)
        .setAciIdentityKey(aciIdentityKey);

    usernameHash.ifPresent(searchRequestBuilder::setUsernameHash);
    e164SearchRequest.ifPresent(searchRequestBuilder::setE164SearchRequest);

    final ConsistencyParameters.Builder consistency = ConsistencyParameters.newBuilder()
        .setDistinguished(distinguishedTreeHeadSize);
    lastTreeHeadSize.ifPresent(consistency::setLast);

    searchRequestBuilder.setConsistency(consistency.build());

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .search(searchRequestBuilder.build()), callbackExecutor)
        .thenApply(AbstractMessageLite::toByteArray);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public CompletableFuture<byte[]> monitor(final AciMonitorRequest aciMonitorRequest,
      final Optional<UsernameHashMonitorRequest> usernameHashMonitorRequest,
      final Optional<E164MonitorRequest> e164MonitorRequest,
      final long lastTreeHeadSize,
      final long distinguishedTreeHeadSize,
      final Duration timeout) {
    final MonitorRequest.Builder monitorRequestBuilder = MonitorRequest.newBuilder()
        .setAci(aciMonitorRequest)
        .setConsistency(ConsistencyParameters.newBuilder()
            .setLast(lastTreeHeadSize)
            .setDistinguished(distinguishedTreeHeadSize)
            .build());

    usernameHashMonitorRequest.ifPresent(monitorRequestBuilder::setUsernameHash);
    e164MonitorRequest.ifPresent(monitorRequestBuilder::setE164);

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .monitor(monitorRequestBuilder.build()), callbackExecutor)
        .thenApply(AbstractMessageLite::toByteArray);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public CompletableFuture<byte[]> getDistinguishedKey(final Optional<Long> lastTreeHeadSize, final Duration timeout) {
    final DistinguishedRequest request = lastTreeHeadSize.map(
            last -> DistinguishedRequest.newBuilder().setLast(last).build())
        .orElseGet(DistinguishedRequest::getDefaultInstance);
    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout)).distinguished(request),
            callbackExecutor)
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
