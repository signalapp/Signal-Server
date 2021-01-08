/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securestorage;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.configuration.SecureStorageServiceConfiguration;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecureStorageClientTest {

    private UUID accountUuid;
    private ExternalServiceCredentialGenerator credentialGenerator;
    private ExecutorService httpExecutor;

    private SecureStorageClient secureStorageClient;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

    @Before
    public void setUp() {
        accountUuid         = UUID.randomUUID();
        credentialGenerator = mock(ExternalServiceCredentialGenerator.class);
        httpExecutor        = Executors.newSingleThreadExecutor();

        final SecureStorageServiceConfiguration config = new SecureStorageServiceConfiguration();
        config.setUri("http://localhost:" + wireMockRule.port());

        secureStorageClient = new SecureStorageClient(credentialGenerator, httpExecutor, config);
    }

    @After
    public void tearDown() throws InterruptedException {
        httpExecutor.shutdown();
        httpExecutor.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void deleteStoredData() {
        final String username = RandomStringUtils.randomAlphabetic(16);
        final String password = RandomStringUtils.randomAlphanumeric(32);

        when(credentialGenerator.generateFor(accountUuid.toString())).thenReturn(new ExternalServiceCredentials(username, password));

        wireMockRule.stubFor(delete(urlEqualTo(SecureStorageClient.DELETE_PATH))
                .withBasicAuth(username, password)
                .willReturn(aResponse().withStatus(202)));

        // We're happy as long as this doesn't throw an exception
        secureStorageClient.deleteStoredData(accountUuid).join();
    }

    @Test
    public void deleteStoredDataFailure() {
        final String username = RandomStringUtils.randomAlphabetic(16);
        final String password = RandomStringUtils.randomAlphanumeric(32);

        when(credentialGenerator.generateFor(accountUuid.toString())).thenReturn(new ExternalServiceCredentials(username, password));

        wireMockRule.stubFor(delete(urlEqualTo(SecureStorageClient.DELETE_PATH))
                .withBasicAuth(username, password)
                .willReturn(aResponse().withStatus(400)));

        assertThrows(RuntimeException.class, () -> secureStorageClient.deleteStoredData(accountUuid).join());
    }
}
