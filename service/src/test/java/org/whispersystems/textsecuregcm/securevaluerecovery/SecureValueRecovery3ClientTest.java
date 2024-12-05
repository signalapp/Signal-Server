/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securevaluerecovery;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery3Configuration;

class SecureValueRecovery3ClientTest {

  private UUID accountUuid;
  private ExternalServiceCredentialsGenerator credentialsGenerator;
  private ExecutorService httpExecutor;
  private ScheduledExecutorService retryExecutor;

  private SecureValueRecovery3Client secureValueRecovery3Client;

  @RegisterExtension
  private static final WireMockExtension backend1WireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  @RegisterExtension
  private static final WireMockExtension backend2WireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  @RegisterExtension
  private static final WireMockExtension backend3WireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  @BeforeEach
  void setUp() throws CertificateException {
    accountUuid = UUID.randomUUID();
    credentialsGenerator = mock(ExternalServiceCredentialsGenerator.class);
    httpExecutor = Executors.newSingleThreadExecutor();
    retryExecutor = Executors.newSingleThreadScheduledExecutor();

    final SecureValueRecovery3Configuration config = new SecureValueRecovery3Configuration(
        "http://localhost:" + backend1WireMock.getPort(),
        "http://localhost:" + backend2WireMock.getPort(),
        "http://localhost:" + backend3WireMock.getPort(),
        randomSecretBytes(32),
        randomSecretBytes(32),
        // This is a randomly-generated, throwaway certificate that's not actually connected to anything
        List.of("""
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
            """, """
            -----BEGIN CERTIFICATE-----
            MIIEpDCCAowCCQC43PUTWSADVjANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
            b2NhbGhvc3QwHhcNMjIxMDE3MjA0NTM0WhcNMjMxMDE3MjA0NTM0WjAUMRIwEAYD
            VQQDDAlsb2NhbGhvc3QwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDV
            x1cdEd2ffQTlTXWRiCHGcrlYf4RJnctt9sw/BuHWTLXBu5LhyJSGn5LRszO/NCXK
            Z/cmGR7pLj366RtiwL+Qo3nhvDCK7T9xZeNIusM6XMcMK9D/DGCYPqtjQz8NXd9V
            ajBBe6nwTDTa+oqX8Mt89foWNkg5Il/lY62u9Dr18LRZ2W9zzYi3Q9/K0CbIX6pM
            yVlPIO5rITOR2IsbeyqsO9jufgX5lP4ZKLLBAP1b7usjC4YdvWacjQg/rK5aay1x
            jC2HCDgo/4N30QVXzSA9nFfSe6AE/xkStK4819JqOkY5JsJCbef1P3hOOdSLEjbp
            xq3MjOs6G6dOgteaAGs10vx7dHxDWETTIiD7BIZ9zRYgOF5bkCaIUO+JfySE1MHD
            KBAFLoRuvmRev5Ln5R0MCHpUMSmMNgJqz+RWZV3g/gpYbuWiHgJOwL1393eK50Bg
            W7SXQ8EjJj2yXZSH+1gPzN0DRoJZiaBoTPnCL2qUgvwFpW1PJsM5FDyUJFUoK5kK
            HLBBSKAPt6ZlSrUe2nBgJv7EF1GK+fTU08LXgW33OpLceGPa0zTShkukQUMtUtZ8
            GqhO12ohMzEupIu5Xurthq4VVUrzHUdj1ZZRMhAbfLU36sd03MMyL/xBqTN6dzCa
            GDGIPGpYjAllZ5xMRt2kZdv+Kr6oo3u2nLUIsqI7KQIDAQABMA0GCSqGSIb3DQEB
            CwUAA4ICAQCB5s43YF35ssf5YONW5iAaifGpi1o0866xfeOybtohFGvQ7V2W34i9
            TYBCt8+0hgatMcvZ08f0vqig1i7nrvYcE1hnhL7JNkU8qm0s9ytHZt6j62nB0kd/
            uqE2hOEQalTf/2TGPV0CCgiqLyd8lEUQvQeA38wktwUeZpVnErlzHeMR2CvV3K8R
            u4vV6SnBcf+TAt56RKYZkPyvZj5llQPo14Glyoo8qZES7Ky1SHmM0GL+baPRBjRW
            3KgSt98Wyu4yr9qu21JpnbAnLhBfzfSKjSeCRgFElUE1GIaFGRZ7ypA74dUKeLnb
            /VUWrszmUhGaEjV9dpI6x6B/kSpQMtIQqBaKRY2ALUeEujS/rURi4iMDwSU+GkSH
            cyEvZKS97OA/dWeXfLXdo4beDBRG93bI4rQnDg5+VdlBOkQSLueb8x6/VThMoC5d
            vZiotFQHseljQAdTkNa6tBu6c4XDYPCKB3CfkMYOlCfTS7Acn5G6dxTPKBtLGBnL
            nQfYyzuwYkN09+2PVzt6auBHr3To7uoclkxX+hxyvPIwIZ0N6b4tQR1FCAkvg29Q
            WIOjZOKGW690ESKCKOnFjUHVO0HpuWnT81URTuY62FXsYdVc2wE4v0E04mEbqQ0P
            lY6ZKNA81Lm3YADYtObmK1IUrOPo9BeIaPy0UM08SmN880Vunqa91Q==
            -----END CERTIFICATE-----
            """),
        null, null);

    secureValueRecovery3Client = new SecureValueRecovery3Client(credentialsGenerator, httpExecutor, retryExecutor,
        config);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    httpExecutor.shutdown();
    httpExecutor.awaitTermination(1, TimeUnit.SECONDS);
    retryExecutor.shutdown();
    retryExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void deleteStoredData() {
    final String username = RandomStringUtils.secure().nextAlphabetic(16);
    final String password = RandomStringUtils.secure().nextAlphanumeric(32);

    when(credentialsGenerator.generateForUuid(accountUuid)).thenReturn(
        new ExternalServiceCredentials(username, password));

    backend1WireMock.stubFor(delete(urlEqualTo(SecureValueRecovery3Client.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(202)));

    backend2WireMock.stubFor(delete(urlEqualTo(SecureValueRecovery3Client.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(202)));

    backend3WireMock.stubFor(delete(urlEqualTo(SecureValueRecovery3Client.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(202)));

    assertDoesNotThrow(() -> secureValueRecovery3Client.deleteBackups(accountUuid).join());
  }

  @ParameterizedTest
  @MethodSource
  void deleteStoredDataFailure(final int backend1Status, final int backend2Status, final int backend3Status) {
    final String username = RandomStringUtils.secure().nextAlphabetic(16);
    final String password = RandomStringUtils.secure().nextAlphanumeric(32);

    when(credentialsGenerator.generateForUuid(accountUuid)).thenReturn(
        new ExternalServiceCredentials(username, password));

    backend1WireMock.stubFor(delete(urlEqualTo(SecureValueRecovery3Client.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(backend1Status)));

    backend2WireMock.stubFor(delete(urlEqualTo(SecureValueRecovery3Client.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(backend2Status)));

    backend3WireMock.stubFor(delete(urlEqualTo(SecureValueRecovery3Client.DELETE_PATH))
        .withBasicAuth(username, password)
        .willReturn(aResponse().withStatus(backend3Status)));

    final CompletionException completionException = assertThrows(CompletionException.class,
        () -> secureValueRecovery3Client.deleteBackups(accountUuid).join());

    assertInstanceOf(SecureValueRecoveryException.class, completionException.getCause());
  }

  private static Stream<Arguments> deleteStoredDataFailure() {
    return Stream.of(
        Arguments.of(400, 202, 202),
        Arguments.of(202, 400, 202),
        Arguments.of(202, 202, 400)
    );
  }

}
