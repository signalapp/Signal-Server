/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securevaluerecovery;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicAuthHeader;

import com.google.api.client.util.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery2Configuration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.util.HttpUtils;

/**
 * A client for sending requests to Signal's secure value recovery v2 service on behalf of authenticated users.
 */
public class SecureValueRecovery2Client implements ValueRecovery2Client {

  private final ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator;
  private final URI deleteUri;
  private final FaultTolerantHttpClient httpClient;

  @VisibleForTesting
  static final String DELETE_PATH = "/v1/delete";

  public SecureValueRecovery2Client(final ExternalServiceCredentialsGenerator secureValueRecoveryCredentialsGenerator,
      final Executor executor, final ScheduledExecutorService retryExecutor,
      final SecureValueRecovery2Configuration configuration)
      throws CertificateException {
    this.secureValueRecoveryCredentialsGenerator = secureValueRecoveryCredentialsGenerator;
    this.deleteUri = URI.create(configuration.uri()).resolve(DELETE_PATH);
    this.httpClient = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(configuration.circuitBreaker())
        .withRetry(configuration.retry())
        .withRetryExecutor(retryExecutor)
        .withVersion(HttpClient.Version.HTTP_1_1)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(HttpClient.Redirect.NEVER)
        .withExecutor(executor)
        .withName("secure-value-recovery")
        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_2)
        .withTrustedServerCertificates(configuration.svrCaCertificates().toArray(new String[0]))
        .build();
  }

  public CompletableFuture<Void> deleteBackups(final UUID accountUuid) {

    final ExternalServiceCredentials credentials = secureValueRecoveryCredentialsGenerator.generateForUuid(accountUuid);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(deleteUri)
        .DELETE()
        .header(HttpHeaders.AUTHORIZATION, basicAuthHeader(credentials))
        .build();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
      if (HttpUtils.isSuccessfulResponse(response.statusCode())) {
        return null;
      }

      throw new SecureValueRecoveryException("Failed to delete backup", String.valueOf(response.statusCode()));
    });
  }

}
