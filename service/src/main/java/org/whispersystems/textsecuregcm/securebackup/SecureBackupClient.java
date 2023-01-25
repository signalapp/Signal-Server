/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securebackup;

import static org.whispersystems.textsecuregcm.util.HeaderUtils.basicAuthHeader;

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
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.util.HttpUtils;

/**
 * A client for sending requests to Signal's secure value recovery service on behalf of authenticated users.
 */
public class SecureBackupClient {

    private final ExternalServiceCredentialsGenerator secureBackupCredentialsGenerator;
    private final URI                                deleteUri;
    private final FaultTolerantHttpClient            httpClient;

    @VisibleForTesting
    static final String DELETE_PATH = "/v1/backup";

    public SecureBackupClient(final ExternalServiceCredentialsGenerator secureBackupCredentialsGenerator, final Executor executor, final SecureBackupServiceConfiguration configuration) throws CertificateException {
        this.secureBackupCredentialsGenerator   = secureBackupCredentialsGenerator;
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
        final ExternalServiceCredentials credentials = secureBackupCredentialsGenerator.generateForUuid(accountUuid);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(deleteUri)
                .DELETE()
                .header(HttpHeaders.AUTHORIZATION, basicAuthHeader(credentials))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
            if (HttpUtils.isSuccessfulResponse(response.statusCode())) {
                return null;
            }

            throw new SecureBackupException("Failed to delete backup: " + response.statusCode());
        });
    }
}
