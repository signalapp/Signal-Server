/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.netty.resolver.dns.DnsNameResolver;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class CloudflareTurnCredentialsManager {

  private static final Logger logger = LoggerFactory.getLogger(CloudflareTurnCredentialsManager.class);

  private static final String CREDENTIAL_FETCH_TIMER_NAME = MetricsUtil.name(CloudflareTurnCredentialsManager.class,
      "credentialFetchLatency");

  private final List<String> cloudflareTurnUrls;
  private final List<String> cloudflareTurnUrlsWithIps;
  private final String cloudflareTurnHostname;
  private final HttpRequest request;

  private final FaultTolerantHttpClient cloudflareTurnClient;
  private final DnsNameResolver dnsNameResolver;

  record CredentialRequest(long ttl) {}

  record CloudflareTurnResponse(IceServer iceServers) {

    record IceServer(
        String username,
        String credential,
        List<String> urls) {
    }
  }

  public CloudflareTurnCredentialsManager(final String cloudflareTurnApiToken,
      final String cloudflareTurnEndpoint, final long cloudflareTurnTtl, final List<String> cloudflareTurnUrls,
      final List<String> cloudflareTurnUrlsWithIps, final String cloudflareTurnHostname,
      final int cloudflareTurnNumHttpClients, final CircuitBreakerConfiguration circuitBreaker,
      final ExecutorService executor, final RetryConfiguration retry, final ScheduledExecutorService retryExecutor,
      final DnsNameResolver dnsNameResolver) {

    this.cloudflareTurnClient = FaultTolerantHttpClient.newBuilder()
        .withName("cloudflare-turn")
        .withCircuitBreaker(circuitBreaker)
        .withExecutor(executor)
        .withRetry(retry)
        .withRetryExecutor(retryExecutor)
        .withNumClients(cloudflareTurnNumHttpClients)
        .build();
    this.cloudflareTurnUrls = cloudflareTurnUrls;
    this.cloudflareTurnUrlsWithIps = cloudflareTurnUrlsWithIps;
    this.cloudflareTurnHostname = cloudflareTurnHostname;
    this.dnsNameResolver = dnsNameResolver;

    try {
      final String body = SystemMapper.jsonMapper().writeValueAsString(new CredentialRequest(cloudflareTurnTtl));
      this.request = HttpRequest.newBuilder()
          .uri(URI.create(cloudflareTurnEndpoint))
          .header("Content-Type", "application/json")
          .header("Authorization", String.format("Bearer %s", cloudflareTurnApiToken))
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .build();
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public TurnToken retrieveFromCloudflare() throws IOException {
    final List<String> cloudflareTurnComposedUrls;
    try {
      cloudflareTurnComposedUrls = dnsNameResolver.resolveAll(cloudflareTurnHostname).get().stream()
          .map(i -> switch (i) {
            case Inet6Address i6 -> "[" + i6.getHostAddress() + "]";
            default -> i.getHostAddress();
          })
          .flatMap(i -> cloudflareTurnUrlsWithIps.stream().map(u -> u.formatted(i)))
          .toList();
    } catch (Exception e) {
      throw new IOException(e);
    }

    final Timer.Sample sample = Timer.start();
    final HttpResponse<String> response;
    try {
      response = cloudflareTurnClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      sample.stop(Timer.builder(CREDENTIAL_FETCH_TIMER_NAME)
          .publishPercentileHistogram(true)
          .tags("outcome", "success")
          .register(Metrics.globalRegistry));
    } catch (CompletionException e) {
      logger.warn("failed to make http request to Cloudflare Turn: {}", e.getMessage());
      sample.stop(Timer.builder(CREDENTIAL_FETCH_TIMER_NAME)
          .publishPercentileHistogram(true)
          .tags("outcome", "failure")
          .register(Metrics.globalRegistry));
      throw new IOException(ExceptionUtils.unwrap(e));
    }

    if (response.statusCode() != Response.Status.CREATED.getStatusCode()) {
      logger.warn("failure request credentials from Cloudflare Turn (code={}): {}", response.statusCode(), response);
      throw new IOException("Cloudflare Turn http failure : " + response.statusCode());
    }

    final CloudflareTurnResponse cloudflareTurnResponse = SystemMapper.jsonMapper()
        .readValue(response.body(), CloudflareTurnResponse.class);

    return TurnTokenGenerator.from(
        cloudflareTurnResponse.iceServers().username(),
        cloudflareTurnResponse.iceServers().credential(),
        Optional.ofNullable(cloudflareTurnUrls),
        Optional.ofNullable(cloudflareTurnComposedUrls),
        cloudflareTurnHostname
    );
  }
}
