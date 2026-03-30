/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.resolver.dns.DnsNameResolver;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class CloudflareTurnCredentialsManager {

  private static final Logger logger = LoggerFactory.getLogger(CloudflareTurnCredentialsManager.class);

  private final List<String> cloudflareTurnUrls;
  private final List<String> cloudflareTurnUrlsWithIps;
  private final String cloudflareTurnHostname;
  private final HttpRequest getCredentialsRequest;

  private final FaultTolerantHttpClient cloudflareTurnClient;
  private final DnsNameResolver dnsNameResolver;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;

  private final Duration clientCredentialTtl;

  private record CredentialRequest(long ttl) {}

  private record CloudflareTurnResponse(IceServer iceServers) {

    private record IceServer(
        String username,
        String credential,
        List<String> urls) {
    }
  }

  public CloudflareTurnCredentialsManager(final String cloudflareTurnApiToken,
      final String cloudflareTurnEndpoint,
      final Duration requestedCredentialTtl,
      final Duration clientCredentialTtl,
      final List<String> cloudflareTurnUrls,
      final List<String> cloudflareTurnUrlsWithIps,
      final String cloudflareTurnHostname,
      final int cloudflareTurnNumHttpClients,
      @Nullable final String circuitBreakerConfigurationName,
      final ExecutorService executor,
      @Nullable final String retryConfigurationName,
      final ScheduledExecutorService retryExecutor,
      final DnsNameResolver dnsNameResolver,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {

    this.cloudflareTurnClient = FaultTolerantHttpClient.newBuilder("cloudflare-turn", executor)
        .withCircuitBreaker(circuitBreakerConfigurationName)
        .withRetry(retryConfigurationName, retryExecutor)
        .withNumClients(cloudflareTurnNumHttpClients)
        .build();
    this.cloudflareTurnUrls = cloudflareTurnUrls;
    this.cloudflareTurnUrlsWithIps = cloudflareTurnUrlsWithIps;
    this.cloudflareTurnHostname = cloudflareTurnHostname;
    this.dnsNameResolver = dnsNameResolver;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.experimentEnrollmentManager = experimentEnrollmentManager;

    final String credentialsRequestBody;

    try {
      credentialsRequestBody =
          SystemMapper.jsonMapper().writeValueAsString(new CredentialRequest(requestedCredentialTtl.toSeconds()));
    } catch (final JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }

    // We repeat the same request to Cloudflare every time, so we can construct it once and re-use it
    this.getCredentialsRequest = HttpRequest.newBuilder()
        .uri(URI.create(cloudflareTurnEndpoint))
        .header("Content-Type", "application/json")
        .header("Authorization", String.format("Bearer %s", cloudflareTurnApiToken))
        .POST(HttpRequest.BodyPublishers.ofString(credentialsRequestBody))
        .build();

    this.clientCredentialTtl = clientCredentialTtl;
  }

  public TurnToken retrieveFromCloudflare(UUID accountUuid) throws IOException {
    final List<String> cloudflareTurnComposedUrls;
    final List<String> turnUrls;
    final List<String> turnUrlsWithIps;
    final String turnHostname;
    if (experimentEnrollmentManager.isEnrolled(accountUuid, "turnBeta")) {
      turnUrls = dynamicConfigurationManager.getConfiguration().getTurnConfiguration().getUrls();
      turnUrlsWithIps = dynamicConfigurationManager.getConfiguration().getTurnConfiguration().getUrlsWithIps();
      turnHostname = dynamicConfigurationManager.getConfiguration().getTurnConfiguration().getHostname();
    } else {
      turnUrls = this.cloudflareTurnUrls;
      turnUrlsWithIps = this.cloudflareTurnUrlsWithIps;
      turnHostname = this.cloudflareTurnHostname;
    }
    try {
      cloudflareTurnComposedUrls = dnsNameResolver.resolveAll(turnHostname).get().stream()
          .map(i -> switch (i) {
            case Inet6Address i6 -> "[" + i6.getHostAddress() + "]";
            default -> i.getHostAddress();
          })
          .flatMap(i -> turnUrlsWithIps.stream().map(u -> u.formatted(i)))
          .toList();
    } catch (Exception e) {
      throw new IOException(e);
    }

    final HttpResponse<String> response;
    try {
      response = cloudflareTurnClient.sendAsync(getCredentialsRequest, HttpResponse.BodyHandlers.ofString()).join();
    } catch (CompletionException e) {
      logger.warn("failed to make http request to Cloudflare Turn: {}", e.getMessage());
      throw new IOException(ExceptionUtils.unwrap(e));
    }

    if (response.statusCode() != Response.Status.CREATED.getStatusCode()) {
      logger.warn("failure request credentials from Cloudflare Turn (code={}): {}", response.statusCode(), response);
      throw new IOException("Cloudflare Turn http failure : " + response.statusCode());
    }

    final CloudflareTurnResponse cloudflareTurnResponse = SystemMapper.jsonMapper()
        .readValue(response.body(), CloudflareTurnResponse.class);

    return new TurnToken(
        cloudflareTurnResponse.iceServers().username(),
        cloudflareTurnResponse.iceServers().credential(),
        clientCredentialTtl.toSeconds(),
        turnUrls,
        cloudflareTurnComposedUrls,
        turnHostname
    );
  }
}
