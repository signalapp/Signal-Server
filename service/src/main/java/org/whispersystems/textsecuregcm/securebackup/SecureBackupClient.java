/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securebackup;

import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

/**
 * A client for sending requests to Signal's secure value recovery service on behalf of authenticated users.
 */
public class SecureBackupClient {

    private final ExternalServiceCredentialGenerator secureBackupCredentialGenerator;
    private final URI                                deleteUri;
    private final FaultTolerantHttpClient            httpClient;

    @VisibleForTesting
    static final String DELETE_PATH = "/v1/backup";

    public SecureBackupClient(final ExternalServiceCredentialGenerator secureBackupCredentialGenerator, final Executor executor, final SecureBackupServiceConfiguration configuration) throws CertificateException {
        this.secureBackupCredentialGenerator   = secureBackupCredentialGenerator;
        this.deleteUri                         = URI.create(configuration.getUri()).resolve(DELETE_PATH);
        this.httpClient                        = FaultTolerantHttpClient.newBuilder()
                                                                        .withCircuitBreaker(configuration.getCircuitBreakerConfiguration())
                                                                        .withRetry(configuration.getRetryConfiguration())
                                                                        .withVersion(HttpClient.Version.HTTP_1_1)
                                                                        .withConnectTimeout(Duration.ofSeconds(10))
                                                                        .withRedirect(HttpClient.Redirect.NEVER)
                                                                        .withExecutor(executor)
                                                                        .withName("secure-backup")
                                                                        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_2)
                                                                        .withTrustedServerCertificates(configuration.getBackupCaCertificates().toArray(new String[0]))
                                                                        .build();
    }

    public CompletableFuture<Void> deleteBackups(final UUID accountUuid) {
        final ExternalServiceCredentials credentials = secureBackupCredentialGenerator.generateFor(accountUuid.toString());

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(deleteUri)
                .DELETE()
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                    (credentials.username() + ":" + credentials.password()).getBytes(StandardCharsets.UTF_8)))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return null;
            }

            throw new SecureBackupException("Failed to delete backup: " + response.statusCode());
        });
    }
}
