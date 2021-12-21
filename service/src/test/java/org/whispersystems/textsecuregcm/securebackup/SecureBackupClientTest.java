/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securebackup;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.security.cert.CertificateException;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;

class SecureBackupClientTest {

  private UUID accountUuid;
  private ExternalServiceCredentialGenerator credentialGenerator;
  private ExecutorService httpExecutor;

  private SecureBackupClient secureStorageClient;

  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  @BeforeEach
  void setUp() throws CertificateException {
    accountUuid         = UUID.randomUUID();
    credentialGenerator = mock(ExternalServiceCredentialGenerator.class);
    httpExecutor        = Executors.newSingleThreadExecutor();

    final SecureBackupServiceConfiguration config = new SecureBackupServiceConfiguration();
    config.setUri("http://localhost:" + wireMock.getPort());

    // This is a randomly-generated, throwaway certificate that's not actually connected to anything
    config.setBackupCaCertificate("""
        -----BEGIN CERTIFICATE-----
        MIICZDCCAc2gAwIBAgIBADANBgkqhkiG9w0BAQ0FADBPMQswCQYDVQQGEwJ1czEL
        MAkGA1UECAwCVVMxHjAcBgNVBAoMFVNpZ25hbCBNZXNzZW5nZXIsIExMQzETMBEG
        A1UEAwwKc2lnbmFsLm9yZzAeFw0yMDEyMjMyMjQ3NTlaFw0zMDEyMjEyMjQ3NTla
        ME8xCzAJBgNVBAYTAnVzMQswCQYDVQQIDAJVUzEeMBwGA1UECgwVU2lnbmFsIE1l
        c3NlbmdlciwgTExDMRMwEQYDVQQDDApzaWduYWwub3JnMIGfMA0GCSqGSIb3DQEB
        AQUAA4GNADCBiQKBgQCfSLcZNHYqbxSsgWp4JvbPRHjQTrlsrKrgD2q7f/OY6O3Y
        /X0QNcNSOJpliN8rmzwslfsrXHO3q1diGRw4xHogUJZ/7NQrHiP/zhN0VTDh49pD
        ZpjXVyUbayLS/6qM5arKxBspzEFBb5v8cF6bPr76SO/rpGXiI0j6yJKX6fRiKwID
        AQABo1AwTjAdBgNVHQ4EFgQU6Jrs/Fmj0z4dA3wvdq/WqA4P49IwHwYDVR0jBBgw
        FoAU6Jrs/Fmj0z4dA3wvdq/WqA4P49IwDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0B
        AQ0FAAOBgQB+5d5+NtzLILfrc9QmJdIO1YeDP64JmFwTER0kEUouRsb9UwknVWZa
        y7MTM4NoBV1k0zb5LAk89SIDPr/maW5AsLtEomzjnEiomjoMBUdNe3YCgQReoLnr
        R/QaUNbrCjTGYfBsjGbIzmkWPUyTec2ZdRyJ8JiVl386+6CZkxnndQ==
        -----END CERTIFICATE-----
        """);

    secureStorageClient = new SecureBackupClient(credentialGenerator, httpExecutor, config);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    httpExecutor.shutdown();
    httpExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void deleteStoredData() {
    final String username = RandomStringUtils.randomAlphabetic(16);
    final String password = RandomStringUtils.randomAlphanumeric(32);

    when(credentialGenerator.generateFor(accountUuid.toString())).thenReturn(new ExternalServiceCredentials(username, password));

    wireMock.stubFor(delete(urlEqualTo(SecureBackupClient.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(202)));

    // We're happy as long as this doesn't throw an exception
    secureStorageClient.deleteBackups(accountUuid).join();
  }

  @Test
  void deleteStoredDataFailure() {
    final String username = RandomStringUtils.randomAlphabetic(16);
    final String password = RandomStringUtils.randomAlphanumeric(32);

    when(credentialGenerator.generateFor(accountUuid.toString())).thenReturn(new ExternalServiceCredentials(username, password));

    wireMock.stubFor(delete(urlEqualTo(SecureBackupClient.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(400)));

    final CompletionException completionException = assertThrows(CompletionException.class, () -> secureStorageClient.deleteBackups(accountUuid).join());
    assertTrue(completionException.getCause() instanceof SecureBackupException);
  }
}
