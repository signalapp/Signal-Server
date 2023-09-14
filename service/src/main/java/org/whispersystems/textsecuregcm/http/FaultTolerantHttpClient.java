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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.glassfish.jersey.SslConfigurator;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.CertificateUtil;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

public class FaultTolerantHttpClient {

  private final HttpClient httpClient;
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
  FaultTolerantHttpClient(String name, HttpClient httpClient, ScheduledExecutorService retryExecutor,
      Duration defaultRequestTimeout, RetryConfiguration retryConfiguration,
      final Predicate<Throwable> retryOnException, CircuitBreakerConfiguration circuitBreakerConfiguration) {

    this.httpClient = httpClient;
    this.retryExecutor = retryExecutor;
    this.defaultRequestTimeout = defaultRequestTimeout;
    this.breaker = CircuitBreaker.of(name + "-breaker", circuitBreakerConfiguration.toCircuitBreakerConfig());

    CircuitBreakerUtil.registerMetrics(breaker, FaultTolerantHttpClient.class);

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

  public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler) {
    if (request.timeout().isEmpty()) {
      request = HttpRequest.newBuilder(request, (n, v) -> true)
          .timeout(defaultRequestTimeout)
          .build();
    }

    Supplier<CompletionStage<HttpResponse<T>>> asyncRequest = sendAsync(httpClient, request, bodyHandler);

    if (retry != null) {
      return breaker.executeCompletionStage(retryableCompletionStage(asyncRequest)).toCompletableFuture();
    } else {
      return breaker.executeCompletionStage(asyncRequest).toCompletableFuture();
    }
  }

  private <T> Supplier<CompletionStage<T>> retryableCompletionStage(Supplier<CompletionStage<T>> supplier) {
    return () -> retry.executeCompletionStage(retryExecutor, supplier);
  }

  private <T> Supplier<CompletionStage<HttpResponse<T>>> sendAsync(HttpClient client, HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler) {
    return () -> client.sendAsync(request, bodyHandler);
  }

  public static class Builder {

    private HttpClient.Version version = HttpClient.Version.HTTP_2;
    private HttpClient.Redirect redirect = HttpClient.Redirect.NEVER;
    private Duration connectTimeout = Duration.ofSeconds(10);
    private Duration requestTimeout = Duration.ofSeconds(60);

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

    public FaultTolerantHttpClient build() {
      if (this.circuitBreakerConfiguration == null || this.name == null || this.executor == null) {
        throw new IllegalArgumentException("Must specify circuit breaker config, name, and executor");
      }

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

      return new FaultTolerantHttpClient(name, builder.build(), retryExecutor, requestTimeout, retryConfiguration,
          retryOnException, circuitBreakerConfiguration);
    }
  }

}
