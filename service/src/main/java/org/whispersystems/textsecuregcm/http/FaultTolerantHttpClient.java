/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.http;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import io.micrometer.core.instrument.Tags;
import org.glassfish.jersey.SslConfigurator;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CertificateUtil;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

public class FaultTolerantHttpClient {

  private final List<HttpClient> httpClients;
  private final Duration defaultRequestTimeout;
  private final ScheduledExecutorService retryExecutor;
  private final Retry retry;
  private final CircuitBreaker breaker;

  public static final String SECURITY_PROTOCOL_TLS_1_2 = "TLSv1.2";
  public static final String SECURITY_PROTOCOL_TLS_1_3 = "TLSv1.3";

  public static Builder newBuilder() {
    return new Builder();
  }

  @VisibleForTesting
  FaultTolerantHttpClient(String name, List<HttpClient> httpClients, ScheduledExecutorService retryExecutor,
      Duration defaultRequestTimeout, RetryConfiguration retryConfiguration,
      final Predicate<Throwable> retryOnException, CircuitBreakerConfiguration circuitBreakerConfiguration) {

    this.httpClients = httpClients;
    this.retryExecutor = retryExecutor;
    this.defaultRequestTimeout = defaultRequestTimeout;
    this.breaker = CircuitBreaker.of(name + "-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());

    CircuitBreakerUtil.registerMetrics(breaker, FaultTolerantHttpClient.class, Tags.empty());

    if (retryConfiguration != null) {
      if (this.retryExecutor == null) {
        throw new IllegalArgumentException("retryExecutor must be specified with retryConfiguration");
      }
      final RetryConfig.Builder<HttpResponse> retryConfig = retryConfiguration.<HttpResponse>toRetryConfigBuilder()
          .retryOnResult(o -> o.statusCode() >= 500);
      if (retryOnException != null) {
        retryConfig.retryOnException(retryOnException);
      }
      this.retry = Retry.of(name + "-retry", retryConfig.build());
      CircuitBreakerUtil.registerMetrics(retry, FaultTolerantHttpClient.class);
    } else {
      this.retry = null;
    }
  }

  private HttpClient httpClient() {
    return this.httpClients.get(ThreadLocalRandom.current().nextInt(this.httpClients.size()));
  }

  public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
      HttpResponse.BodyHandler<T> bodyHandler) {
    if (request.timeout().isEmpty()) {
      request = HttpRequest.newBuilder(request, (n, v) -> true)
          .timeout(defaultRequestTimeout)
          .build();
    }

    Supplier<CompletionStage<HttpResponse<T>>> asyncRequest = sendAsync(httpClient(), request, bodyHandler);

    if (retry != null) {
      return breaker.executeCompletionStage(retryableCompletionStage(asyncRequest)).toCompletableFuture();
    } else {
      return breaker.executeCompletionStage(asyncRequest).toCompletableFuture();
    }
  }

  private <T> Supplier<CompletionStage<T>> retryableCompletionStage(Supplier<CompletionStage<T>> supplier) {
    return () -> retry.executeCompletionStage(retryExecutor, supplier);
  }

  private <T> Supplier<CompletionStage<HttpResponse<T>>> sendAsync(HttpClient client, HttpRequest request,
      HttpResponse.BodyHandler<T> bodyHandler) {
    return () -> client.sendAsync(request, bodyHandler);
  }

  public static class Builder {

    private HttpClient.Version version = HttpClient.Version.HTTP_2;
    private HttpClient.Redirect redirect = HttpClient.Redirect.NEVER;
    private Duration connectTimeout = Duration.ofSeconds(10);
    private Duration requestTimeout = Duration.ofSeconds(60);
    private int numClients = 1;

    private String name;
    private Executor executor;
    private ScheduledExecutorService retryExecutor;
    private KeyStore trustStore;
    private String securityProtocol = SECURITY_PROTOCOL_TLS_1_2;
    private RetryConfiguration retryConfiguration;
    private Predicate<Throwable> retryOnException;
    private CircuitBreakerConfiguration circuitBreakerConfiguration;

    private Builder() {
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withVersion(HttpClient.Version version) {
      this.version = version;
      return this;
    }

    public Builder withRedirect(HttpClient.Redirect redirect) {
      this.redirect = redirect;
      return this;
    }

    public Builder withExecutor(Executor executor) {
      this.executor = executor;
      return this;
    }

    public Builder withConnectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder withRequestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    public Builder withRetry(RetryConfiguration retryConfiguration) {
      this.retryConfiguration = retryConfiguration;
      return this;
    }

    public Builder withRetryExecutor(ScheduledExecutorService retryExecutor) {
      this.retryExecutor = retryExecutor;
      return this;
    }

    public Builder withCircuitBreaker(CircuitBreakerConfiguration circuitBreakerConfiguration) {
      this.circuitBreakerConfiguration = circuitBreakerConfiguration;
      return this;
    }

    public Builder withSecurityProtocol(final String securityProtocol) {
      this.securityProtocol = securityProtocol;
      return this;
    }

    public Builder withTrustedServerCertificates(final String... certificatePem) throws CertificateException {
      this.trustStore = CertificateUtil.buildKeyStoreForPem(certificatePem);
      return this;
    }

    public Builder withRetryOnException(final Predicate<Throwable> predicate) {
      this.retryOnException = throwable -> predicate.test(ExceptionUtils.unwrap(throwable));
      return this;
    }

    /**
     * Specify that the HttpClient should stripe requests across multiple HTTP clients
     * <p>
     * A {@link java.net.http.HttpClient} configured to use HTTP/2 will open a single connection per target host and
     * will send concurrent requests to that host over the same connection. If the target host has set a low HTTP/2
     * MAX_CONCURRENT_STREAMS, at MAX_CONCURRENT_STREAMS concurrent requests the client will throw IOExceptions.
     * <p>
     * To use a higher parallelism than the host sets per connection, setting a higher numClients will increase the
     * number of connections we make to the backing server. Each request will be assigned to a random client.
     * <p>
     * This builder will refuse to {@link #build()} if the HTTP version is not HTTP/2
     *
     * @param numClients The number of underlying HTTP clients to use
     * @return {@code this}
     */
    public Builder withNumClients(final int numClients) {
      this.numClients = numClients;
      return this;
    }

    public FaultTolerantHttpClient build() {
      if (this.circuitBreakerConfiguration == null || this.name == null || this.executor == null) {
        throw new IllegalArgumentException("Must specify circuit breaker config, name, and executor");
      }

      if (numClients > 1 && version != HttpClient.Version.HTTP_2) {
        throw new IllegalArgumentException("Should not use additional HTTP clients unless using HTTP/2");
      }

      final List<HttpClient> httpClients = IntStream
          .range(0, numClients)
          .mapToObj(i -> {
            final HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .followRedirects(redirect)
                .version(version)
                .executor(executor);

            final SslConfigurator sslConfigurator = SslConfigurator.newInstance().securityProtocol(securityProtocol);

            if (this.trustStore != null) {
              sslConfigurator.trustStore(trustStore);
            }
            builder.sslContext(sslConfigurator.createSSLContext());
            return builder.build();
          }).toList();

      return new FaultTolerantHttpClient(name, httpClients, retryExecutor, requestTimeout, retryConfiguration,
          retryOnException, circuitBreakerConfiguration);
    }
  }

}
