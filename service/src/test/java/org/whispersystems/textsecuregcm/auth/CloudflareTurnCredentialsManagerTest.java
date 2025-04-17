/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.SucceededFuture;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

public class CloudflareTurnCredentialsManagerTest {
  @RegisterExtension
  private static final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private ExecutorService httpExecutor;
  private ScheduledExecutorService retryExecutor;
  private DnsNameResolver dnsResolver;

  private CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager;

  private static final String GET_CREDENTIALS_PATH = "/v1/turn/keys/LMNOP/credentials/generate";
  private static final String TURN_HOSTNAME = "localhost";

  private static final String API_TOKEN = RandomStringUtils.insecure().nextAlphanumeric(16);
  private static final String USERNAME = RandomStringUtils.insecure().nextAlphanumeric(16);
  private static final String CREDENTIAL = RandomStringUtils.insecure().nextAlphanumeric(16);
  private static final List<String> CLOUDFLARE_TURN_URLS = List.of("turn:cf.example.com");
  private static final Duration REQUESTED_CREDENTIAL_TTL = Duration.ofSeconds(100);
  private static final Duration CLIENT_CREDENTIAL_TTL = REQUESTED_CREDENTIAL_TTL.dividedBy(2);
  private static final List<String> IP_URL_PATTERNS = List.of("turn:%s", "turn:%s:80?transport=tcp", "turns:%s:443?transport=tcp");

  @BeforeEach
  void setUp() {
    httpExecutor = Executors.newSingleThreadExecutor();
    retryExecutor = Executors.newSingleThreadScheduledExecutor();

    dnsResolver = mock(DnsNameResolver.class);

    cloudflareTurnCredentialsManager = new CloudflareTurnCredentialsManager(
        API_TOKEN,
        "http://localhost:" + wireMock.getPort() + GET_CREDENTIALS_PATH,
        REQUESTED_CREDENTIAL_TTL,
        CLIENT_CREDENTIAL_TTL,
        CLOUDFLARE_TURN_URLS,
        IP_URL_PATTERNS,
        TURN_HOSTNAME,
        2,
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
    retryExecutor.shutdown();

    //noinspection ResultOfMethodCallIgnored
    httpExecutor.awaitTermination(1, TimeUnit.SECONDS);

    //noinspection ResultOfMethodCallIgnored
    retryExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testSuccess() throws IOException, CancellationException {
    wireMock.stubFor(post(urlEqualTo(GET_CREDENTIALS_PATH))
        .willReturn(created()
            .withHeader("Content-Type", "application/json")
            .withBody("""
                {
                   "iceServers": {
                     "urls": [
                       "turn:cloudflare.example.com:3478?transport=udp"
                     ],
                     "username": "%s",
                     "credential": "%s"
                   }
                 }
                """.formatted(USERNAME, CREDENTIAL))));

    when(dnsResolver.resolveAll(TURN_HOSTNAME))
        .thenReturn(new SucceededFuture<>(GlobalEventExecutor.INSTANCE,
            List.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1"))));

    TurnToken token = cloudflareTurnCredentialsManager.retrieveFromCloudflare();

    wireMock.verify(postRequestedFor(urlEqualTo(GET_CREDENTIALS_PATH))
        .withHeader("Content-Type", equalTo("application/json"))
        .withHeader("Authorization", equalTo("Bearer " + API_TOKEN))
        .withRequestBody(equalToJson("""
            {
              "ttl": %d
            }
            """.formatted(REQUESTED_CREDENTIAL_TTL.toSeconds()))));

    assertThat(token.username()).isEqualTo(USERNAME);
    assertThat(token.password()).isEqualTo(CREDENTIAL);
    assertThat(token.hostname()).isEqualTo(TURN_HOSTNAME);
    assertThat(token.urls()).isEqualTo(CLOUDFLARE_TURN_URLS);
    assertThat(token.ttlSeconds()).isEqualTo(CLIENT_CREDENTIAL_TTL.toSeconds());

    final List<String> expectedUrlsWithIps = new ArrayList<>();

    for (final String ip : new String[] {"127.0.0.1", "[0:0:0:0:0:0:0:1]"}) {
      for (final String pattern : IP_URL_PATTERNS) {
        expectedUrlsWithIps.add(pattern.formatted(ip));
      }
    }

    assertThat(token.urlsWithIps()).containsExactlyElementsOf(expectedUrlsWithIps);
  }
}
