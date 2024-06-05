/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.net.InetAddress;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

public class CloudflareTurnCredentialsManagerTest {
  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private static final String GET_CREDENTIALS_PATH = "/v1/turn/keys/LMNOP/credentials/generate";
  private static final String TURN_HOSTNAME = "localhost";
  private ExecutorService httpExecutor;
  private ScheduledExecutorService retryExecutor;
  private DnsNameResolver dnsResolver;
  private Future<List<InetAddress>> dnsResult;

  private CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager = null;

  @BeforeEach
  void setUp() throws CertificateException {
    httpExecutor = Executors.newSingleThreadExecutor();
    retryExecutor = Executors.newSingleThreadScheduledExecutor();
    dnsResolver = mock(DnsNameResolver.class);
    dnsResult = mock(Future.class);
    cloudflareTurnCredentialsManager = new CloudflareTurnCredentialsManager(
        "API_TOKEN",
        "http://localhost:" + wireMock.getPort() + GET_CREDENTIALS_PATH,
        100,
        List.of("turn:cf.example.com"),
        List.of("turn:%s", "turn:%s:80?transport=tcp", "turns:%s:443?transport=tcp"),
        TURN_HOSTNAME,
        new CircuitBreakerConfiguration(),
        httpExecutor,
        new RetryConfiguration(),
        retryExecutor,
        dnsResolver
    );
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    httpExecutor.shutdown();
    httpExecutor.awaitTermination(1, TimeUnit.SECONDS);
    retryExecutor.shutdown();
    retryExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testSuccess() throws IOException, CancellationException, ExecutionException, InterruptedException {
    wireMock.stubFor(post(urlEqualTo(GET_CREDENTIALS_PATH))
        .willReturn(aResponse().withStatus(201).withHeader("Content-Type", new String[]{"application/json"}).withBody("{\"iceServers\":{\"urls\":[\"turn:cloudflare.example.com:3478?transport=udp\"],\"username\":\"ABC\",\"credential\":\"XYZ\"}}")));
    when(dnsResult.get())
        .thenReturn(List.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1")));
    when(dnsResolver.resolveAll(TURN_HOSTNAME))
        .thenReturn(dnsResult);

    TurnToken token = cloudflareTurnCredentialsManager.retrieveFromCloudflare();

    assertThat(token.username()).isEqualTo("ABC");
    assertThat(token.password()).isEqualTo("XYZ");
    assertThat(token.hostname()).isEqualTo("localhost");
    assertThat(token.urlsWithIps()).containsAll(List.of("turn:127.0.0.1", "turn:127.0.0.1:80?transport=tcp", "turns:127.0.0.1:443?transport=tcp", "turn:[0:0:0:0:0:0:0:1]", "turn:[0:0:0:0:0:0:0:1]:80?transport=tcp", "turns:[0:0:0:0:0:0:0:1]:443?transport=tcp"));;
    assertThat(token.urls()).isEqualTo(List.of("turn:cf.example.com"));
  }
}
