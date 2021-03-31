/*
 * Copyright 2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securebackup;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.lang3.RandomStringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;

import java.security.Security;
import java.security.cert.CertificateException;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecureBackupClientTest {

  private UUID accountUuid;
  private ExternalServiceCredentialGenerator credentialGenerator;
  private ExecutorService httpExecutor;

  private SecureBackupClient secureStorageClient;

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

  @BeforeClass
  public static void setupBeforeClass() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @Before
  public void setUp() throws CertificateException {
    accountUuid         = UUID.randomUUID();
    credentialGenerator = mock(ExternalServiceCredentialGenerator.class);
    httpExecutor        = Executors.newSingleThreadExecutor();

    final SecureBackupServiceConfiguration config = new SecureBackupServiceConfiguration();
    config.setUri("http://localhost:" + wireMockRule.port());

    // This is a randomly-generated, throwaway certificate that's not actually connected to anything
    config.setBackupCaCertificate(
        "-----BEGIN CERTIFICATE-----\n" +
            "MIICZDCCAc2gAwIBAgIBADANBgkqhkiG9w0BAQ0FADBPMQswCQYDVQQGEwJ1czEL\n" +
            "MAkGA1UECAwCVVMxHjAcBgNVBAoMFVNpZ25hbCBNZXNzZW5nZXIsIExMQzETMBEG\n" +
            "A1UEAwwKc2lnbmFsLm9yZzAeFw0yMDEyMjMyMjQ3NTlaFw0zMDEyMjEyMjQ3NTla\n" +
            "ME8xCzAJBgNVBAYTAnVzMQswCQYDVQQIDAJVUzEeMBwGA1UECgwVU2lnbmFsIE1l\n" +
            "c3NlbmdlciwgTExDMRMwEQYDVQQDDApzaWduYWwub3JnMIGfMA0GCSqGSIb3DQEB\n" +
            "AQUAA4GNADCBiQKBgQCfSLcZNHYqbxSsgWp4JvbPRHjQTrlsrKrgD2q7f/OY6O3Y\n" +
            "/X0QNcNSOJpliN8rmzwslfsrXHO3q1diGRw4xHogUJZ/7NQrHiP/zhN0VTDh49pD\n" +
            "ZpjXVyUbayLS/6qM5arKxBspzEFBb5v8cF6bPr76SO/rpGXiI0j6yJKX6fRiKwID\n" +
            "AQABo1AwTjAdBgNVHQ4EFgQU6Jrs/Fmj0z4dA3wvdq/WqA4P49IwHwYDVR0jBBgw\n" +
            "FoAU6Jrs/Fmj0z4dA3wvdq/WqA4P49IwDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0B\n" +
            "AQ0FAAOBgQB+5d5+NtzLILfrc9QmJdIO1YeDP64JmFwTER0kEUouRsb9UwknVWZa\n" +
            "y7MTM4NoBV1k0zb5LAk89SIDPr/maW5AsLtEomzjnEiomjoMBUdNe3YCgQReoLnr\n" +
            "R/QaUNbrCjTGYfBsjGbIzmkWPUyTec2ZdRyJ8JiVl386+6CZkxnndQ==\n" +
            "-----END CERTIFICATE-----");

    secureStorageClient = new SecureBackupClient(credentialGenerator, httpExecutor, config);
  }

  @After
  public void tearDown() throws InterruptedException {
    httpExecutor.shutdown();
    httpExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
  }

  @Test
  public void deleteStoredData() {
    final String username = RandomStringUtils.randomAlphabetic(16);
    final String password = RandomStringUtils.randomAlphanumeric(32);

    when(credentialGenerator.generateFor(accountUuid.toString())).thenReturn(new ExternalServiceCredentials(username, password));

    wireMockRule.stubFor(delete(urlEqualTo(SecureBackupClient.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(202)));

    // We're happy as long as this doesn't throw an exception
    secureStorageClient.deleteBackups(accountUuid).join();
  }

  @Test
  public void deleteStoredDataFailure() {
    final String username = RandomStringUtils.randomAlphabetic(16);
    final String password = RandomStringUtils.randomAlphanumeric(32);

    when(credentialGenerator.generateFor(accountUuid.toString())).thenReturn(new ExternalServiceCredentials(username, password));

    wireMockRule.stubFor(delete(urlEqualTo(SecureBackupClient.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(400)));

    final CompletionException completionException = assertThrows(CompletionException.class, () -> secureStorageClient.deleteBackups(accountUuid).join());
    assertTrue(completionException.getCause() instanceof SecureBackupException);
  }
}
