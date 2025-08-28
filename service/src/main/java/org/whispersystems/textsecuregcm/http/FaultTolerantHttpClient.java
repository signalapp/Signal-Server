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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.glassfish.jersey.SslConfigurator;
import org.whispersystems.textsecuregcm.util.CertificateUtil;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

public class FaultTolerantHttpClient {

  private final List<HttpClient> httpClients;
  private final Duration defaultRequestTimeout;
  @Nullable private final ScheduledExecutorService retryExecutor;
  @Nullable private final Retry retry;
  private final CircuitBreaker breaker;

  public static final String SECURITY_PROTOCOL_TLS_1_2 = "TLSv1.2";
  public static final String SECURITY_PROTOCOL_TLS_1_3 = "TLSv1.3";

  public static Builder newBuilder(final String name, final Executor executor) {
    return new Builder(name, executor);
  }

  @VisibleForTesting
  FaultTolerantHttpClient(final List<HttpClient> httpClients,
      final Duration defaultRequestTimeout,
      @Nullable final ScheduledExecutorService retryExecutor,
      @Nullable final Retry retry,
      final CircuitBreaker circuitBreaker) {

    this.httpClients = httpClients;
    this.defaultRequestTimeout = defaultRequestTimeout;
    this.retryExecutor = retryExecutor;
    this.retry = retry;
    this.breaker = circuitBreaker;
  }

  private HttpClient httpClient() {
    return this.httpClients.get(ThreadLocalRandom.current().nextInt(this.httpClients.size()));
  }

  public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
      final HttpResponse.BodyHandler<T> bodyHandler) {

    if (request.timeout().isEmpty()) {
      request = HttpRequest.newBuilder(request, (_, _) -> true)
          .timeout(defaultRequestTimeout)
          .build();
    }

    final Supplier<CompletionStage<HttpResponse<T>>> asyncRequestSupplier =
        sendAsync(httpClient(), request, bodyHandler);

    if (retry != null) {
      assert retryExecutor != null;

      return breaker.executeCompletionStage(retry.decorateCompletionStage(retryExecutor, asyncRequestSupplier))
          .toCompletableFuture();
    } else {
      return breaker.executeCompletionStage(asyncRequestSupplier).toCompletableFuture();
    }
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

    private final String name;
    private Executor executor;
    private KeyStore trustStore;
    private String securityProtocol = SECURITY_PROTOCOL_TLS_1_2;
    private String retryConfigurationName;
    private ScheduledExecutorService retryExecutor;
    private Predicate<Throwable> retryOnException;
    @Nullable private String circuitBreakerConfigurationName;

    private Builder(final String name, final Executor executor) {
      this.name = getClass().getSimpleName() + "/" + Objects.requireNonNull(name);
      this.executor = Objects.requireNonNull(executor);
    }

    public Builder withVersion(HttpClient.Version version) {
      this.version = version;
      return this;
    }

    public Builder withRedirect(HttpClient.Redirect redirect) {
      this.redirect = redirect;
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

    public Builder withRetry(@Nullable final String retryConfigurationName, final ScheduledExecutorService retryExecutor) {
      this.retryConfigurationName = retryConfigurationName;
      this.retryExecutor = retryExecutor;

      return this;
    }

    public Builder withCircuitBreaker(@Nullable final String circuitBreakerConfigurationName) {
      this.circuitBreakerConfigurationName = circuitBreakerConfigurationName;
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

      @Nullable final Retry retry;

      if (retryExecutor != null) {
        final RetryConfig.Builder<HttpResponse<?>> retryConfigBuilder =
            RetryConfig.from(Optional.ofNullable(retryConfigurationName)
                .flatMap(name -> ResilienceUtil.getRetryRegistry().getConfiguration(name))
                .orElseGet(() -> ResilienceUtil.getRetryRegistry().getDefaultConfig()));

        retryConfigBuilder.retryOnResult(response -> response.statusCode() >= 500);

        if (retryOnException != null) {
          retryConfigBuilder.retryOnException(retryOnException);
        }

        retry = ResilienceUtil.getRetryRegistry()
            .retry(ResilienceUtil.name(FaultTolerantHttpClient.class, name), retryConfigBuilder.build());
      } else {
        retry = null;
      }

      final String circuitBreakerName = ResilienceUtil.name(FaultTolerantHttpClient.class, name);

      final CircuitBreaker circuitBreaker = circuitBreakerConfigurationName != null
          ? ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(circuitBreakerName, circuitBreakerConfigurationName)
          : ResilienceUtil.getCircuitBreakerRegistry().circuitBreaker(circuitBreakerName);

      return new FaultTolerantHttpClient(httpClients, requestTimeout, retryExecutor, retry, circuitBreaker);
    }
  }

}
