/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securevaluerecovery;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicAuthHeader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
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
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecoveryConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.util.HttpUtils;

/**
 * A client for sending requests to Signal's secure value recovery service on behalf of authenticated users.
 */
public class SecureValueRecoveryClient {

  private static final Logger logger = LoggerFactory.getLogger(SecureValueRecoveryClient.class);

  private final ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator;
  private final URI deleteUri;
  private final Supplier<List<Integer>> allowedDeletionErrorStatusCodes;
  private final FaultTolerantHttpClient httpClient;

  @VisibleForTesting
  static final String DELETE_PATH = "/v1/delete";

  public SecureValueRecoveryClient(
      final ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator,
      final Executor executor, final ScheduledExecutorService retryExecutor,
      final SecureValueRecoveryConfiguration configuration,
      Supplier<List<Integer>> allowedDeletionErrorStatusCodes)
      throws CertificateException {
    this.secureValueRecoveryCredentialsGenerator = secureValueRecoveryCredentialsGenerator;
    this.deleteUri = URI.create(configuration.uri()).resolve(DELETE_PATH);
    this.allowedDeletionErrorStatusCodes = allowedDeletionErrorStatusCodes;
    this.httpClient = FaultTolerantHttpClient.newBuilder("secure-value-recovery", executor)
        .withCircuitBreaker(configuration.circuitBreakerConfigurationName())
        .withRetry(configuration.retryConfigurationName(), retryExecutor)
        .withVersion(HttpClient.Version.HTTP_1_1)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(HttpClient.Redirect.NEVER)
        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_2)
        .withTrustedServerCertificates(configuration.svrCaCertificates().toArray(new String[0]))
        .build();
  }

  public CompletableFuture<Void> removeData(final UUID accountUuid) {
    return removeData(accountUuid.toString());
  }

  public CompletableFuture<Void> removeData(final String userIdentifier) {

    final ExternalServiceCredentials credentials = secureValueRecoveryCredentialsGenerator.generateFor(userIdentifier);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(deleteUri)
        .DELETE()
        .header(HttpHeaders.AUTHORIZATION, basicAuthHeader(credentials))
        .build();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
      if (HttpUtils.isSuccessfulResponse(response.statusCode())) {
        return null;
      }

      final List<Integer> allowedErrors = allowedDeletionErrorStatusCodes.get();
      if (allowedErrors.contains(response.statusCode())) {
        logger.warn("Ignoring failure to delete svr entry for identifier {} with status {}",
            userIdentifier, response.statusCode());
        return null;
      }
      logger.warn("Failed to delete svr entry for identifier {} with status {}", userIdentifier, response.statusCode());
      throw new SecureValueRecoveryException("Failed to delete backup", String.valueOf(response.statusCode()));
    });
  }

}
