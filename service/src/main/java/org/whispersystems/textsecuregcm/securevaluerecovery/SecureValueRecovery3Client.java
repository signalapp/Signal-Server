/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securevaluerecovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery3Configuration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HttpUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicAuthHeader;

/**
 * A client for sending requests to Signal's secure value recovery v3 service on behalf of authenticated users.
 */
public class SecureValueRecovery3Client {
  private final ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator;
  private final URI backend1Uri;
  private final URI backend2Uri;
  private final URI backend3Uri;

  private final FaultTolerantHttpClient httpClient;

  @VisibleForTesting
  static final String DELETE_PATH = "/v1/delete";

  public SecureValueRecovery3Client(final ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator,
      final Executor executor, final ScheduledExecutorService retryExecutor,
      final SecureValueRecovery3Configuration configuration)
      throws CertificateException {
    this.secureValueRecoveryCredentialsGenerator = secureValueRecoveryCredentialsGenerator;
    this.backend1Uri = URI.create(configuration.backend1Uri()).resolve(DELETE_PATH);
    this.backend2Uri = URI.create(configuration.backend2Uri()).resolve(DELETE_PATH);
    this.backend3Uri = URI.create(configuration.backend3Uri()).resolve(DELETE_PATH);
    this.httpClient = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(configuration.circuitBreaker())
        .withRetry(configuration.retry())
        .withRetryExecutor(retryExecutor)
        .withVersion(HttpClient.Version.HTTP_1_1)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(HttpClient.Redirect.NEVER)
        .withExecutor(executor)
        .withName("secure-value-recovery3")
        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_2)
        .withTrustedServerCertificates(configuration.svrCaCertificates().toArray(String[]::new))
        .build();
  }

  public CompletableFuture<Void> deleteBackups(final UUID accountUuid) {
    final ExternalServiceCredentials credentials = secureValueRecoveryCredentialsGenerator.generateForUuid(accountUuid);
    final List<CompletableFuture<HttpResponse<String>>> futures = Stream.of(backend1Uri, backend2Uri, backend3Uri)
        .map(uri -> HttpRequest.newBuilder()
            .uri(uri)
            .DELETE()
            .header(HttpHeaders.AUTHORIZATION, basicAuthHeader(credentials))
            .build())
        .map(request -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()))
        .toList();

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(ignored -> futures.stream().map(CompletableFuture::join).toList())
        .thenAccept(responses -> responses.forEach(response -> {
          if (!HttpUtils.isSuccessfulResponse(response.statusCode())) {
            throw new SecureValueRecoveryException(String.format("Failed to delete backup in %s", response.uri()), String.valueOf(response.statusCode()));
          }
        }));
  }
}
