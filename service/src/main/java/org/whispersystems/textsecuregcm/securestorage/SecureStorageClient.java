/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securestorage;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.configuration.SecureStorageServiceConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

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

/**
 * A client for sending requests to Signal's secure storage service on behalf of authenticated users.
 */
public class SecureStorageClient {

    private final ExternalServiceCredentialGenerator storageServiceCredentialGenerator;
    private final URI                                deleteUri;
    private final FaultTolerantHttpClient            httpClient;

    @VisibleForTesting
    static final String DELETE_PATH = "/v1/storage";

    public SecureStorageClient(final ExternalServiceCredentialGenerator storageServiceCredentialGenerator, final Executor executor, final SecureStorageServiceConfiguration configuration) throws CertificateException {
        this.storageServiceCredentialGenerator = storageServiceCredentialGenerator;
        this.deleteUri                         = URI.create(configuration.getUri()).resolve(DELETE_PATH);
        this.httpClient                        = FaultTolerantHttpClient.newBuilder()
                                                                        .withCircuitBreaker(configuration.getCircuitBreakerConfiguration())
                                                                        .withRetry(configuration.getRetryConfiguration())
                                                                        .withVersion(HttpClient.Version.HTTP_1_1)
                                                                        .withConnectTimeout(Duration.ofSeconds(10))
                                                                        .withRedirect(HttpClient.Redirect.NEVER)
                                                                        .withExecutor(executor)
                                                                        .withName("secure-storage")
                                                                        .withSecurityProtocol(FaultTolerantHttpClient.SECURITY_PROTOCOL_TLS_1_3)
                                                                        .withTrustedServerCertificates(configuration.getStorageCaCertificates().toArray(new String[0]))
                                                                        .build();
    }

    public CompletableFuture<Void> deleteStoredData(final UUID accountUuid) {
        final ExternalServiceCredentials credentials = storageServiceCredentialGenerator.generateFor(accountUuid.toString());

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

            throw new SecureStorageException("Failed to delete storage service data: " + response.statusCode());
        });
    }
}
