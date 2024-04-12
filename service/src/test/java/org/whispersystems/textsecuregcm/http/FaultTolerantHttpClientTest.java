/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

class FaultTolerantHttpClientTest {

  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private ExecutorService httpExecutor;
  private ScheduledExecutorService retryExecutor;

  @BeforeEach
  void setUp() {
    httpExecutor = Executors.newSingleThreadExecutor();
    retryExecutor = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    httpExecutor.shutdown();
    httpExecutor.awaitTermination(1, TimeUnit.SECONDS);
    retryExecutor.shutdown();
    retryExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  void testSimpleGet() {
    wireMock.stubFor(get(urlEqualTo("/ping"))
        .willReturn(aResponse()
            .withHeader("Content-Type", "text/plain")
            .withBody("Pong!")));

    FaultTolerantHttpClient client = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(new CircuitBreakerConfiguration())
        .withRetry(new RetryConfiguration())
        .withExecutor(httpExecutor)
        .withRetryExecutor(retryExecutor)
        .withName("test")
        .withVersion(HttpClient.Version.HTTP_2)
        .build();

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(URI.create("http://localhost:" + wireMock.getPort() + "/ping"))
                                     .GET()
                                     .build();

    HttpResponse<String> response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("Pong!");

    wireMock.verify(1, getRequestedFor(urlEqualTo("/ping")));
  }

  @Test
  void testRetryGet() {
    wireMock.stubFor(get(urlEqualTo("/failure"))
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "text/plain")
                                             .withBody("Pong!")));

    FaultTolerantHttpClient client = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(new CircuitBreakerConfiguration())
        .withRetry(new RetryConfiguration())
        .withExecutor(httpExecutor)
        .withRetryExecutor(retryExecutor)
        .withName("test")
        .withVersion(HttpClient.Version.HTTP_2)
        .build();

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(URI.create("http://localhost:" + wireMock.getPort() + "/failure"))
                                     .GET()
                                     .build();

    HttpResponse<String> response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();

    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body()).isEqualTo("Pong!");

    wireMock.verify(3, getRequestedFor(urlEqualTo("/failure")));
  }

  @Test
  void testRetryGetOnException() {
    final HttpClient mockHttpClient = mock(HttpClient.class);
    final FaultTolerantHttpClient client = new FaultTolerantHttpClient(
        "test",
        List.of(mockHttpClient),
        retryExecutor,
        Duration.ofSeconds(1),
        new RetryConfiguration(),
        throwable -> throwable instanceof IOException,
        new CircuitBreakerConfiguration());

    when(mockHttpClient.sendAsync(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new IOException("test exception")));

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:1234/failure"))
        .GET()
        .build();

    try {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      throw new AssertionError("Should have failed!");
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(IOException.class);
    }
    verify(mockHttpClient, times(3)).sendAsync(any(), any());
  }

  @Test
  void testMultipleClients() throws IOException, InterruptedException {
    final HttpClient mockHttpClient1 = mock(HttpClient.class);
    final HttpClient mockHttpClient2 = mock(HttpClient.class);
    final FaultTolerantHttpClient client = new FaultTolerantHttpClient(
        "test",
        List.of(mockHttpClient1, mockHttpClient2),
        retryExecutor,
        Duration.ofSeconds(1),
        new RetryConfiguration(),
        throwable -> throwable instanceof IOException,
        new CircuitBreakerConfiguration());

    // Just to get a dummy HttpResponse
    wireMock.stubFor(get(urlEqualTo("/ping"))
        .willReturn(aResponse()
            .withHeader("Content-Type", "text/plain")
            .withBody("Pong!")));

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + wireMock.getPort() + "/ping"))
        .GET()
        .build();
    final HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());

    final AtomicInteger client1Calls = new AtomicInteger(0);
    final AtomicInteger client2Calls = new AtomicInteger(0);
    when(mockHttpClient1.sendAsync(any(), any()))
        .thenAnswer(args -> {
          client1Calls.incrementAndGet();
          return CompletableFuture.completedFuture(response);
        });
    when(mockHttpClient2.sendAsync(any(), any()))
        .thenAnswer(args -> {
          client2Calls.incrementAndGet();
          return CompletableFuture.completedFuture(response);
        });

    final int numCalls = 100;
    for (int i = 0; i < numCalls; i++) {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
    }
    assertThat(client2Calls.get()).isGreaterThan(0);
    assertThat(client1Calls.get()).isGreaterThan(0);
    assertThat(client1Calls.get() + client2Calls.get()).isEqualTo(numCalls);
  }

  @Test
  void testNetworkFailureCircuitBreaker() throws InterruptedException {
    CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();
    circuitBreakerConfiguration.setSlidingWindowSize(2);
    circuitBreakerConfiguration.setSlidingWindowMinimumNumberOfCalls(2);
    circuitBreakerConfiguration.setPermittedNumberOfCallsInHalfOpenState(1);
    circuitBreakerConfiguration.setFailureRateThreshold(50);
    circuitBreakerConfiguration.setWaitDurationInOpenState(Duration.ofSeconds(1));

    FaultTolerantHttpClient client = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(circuitBreakerConfiguration)
        .withRetry(new RetryConfiguration())
        .withRetryExecutor(retryExecutor)
        .withExecutor(httpExecutor)
        .withName("test")
        .withVersion(HttpClient.Version.HTTP_2)
        .build();

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(URI.create("http://localhost:" + 39873 + "/failure"))
                                     .GET()
                                     .build();

    try {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      throw new AssertionError("Should have failed!");
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(IOException.class);
      // good
    }

    try {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      throw new AssertionError("Should have failed!");
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(IOException.class);
      // good
    }

    try {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      throw new AssertionError("Should have failed!");
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(CallNotPermittedException.class);
      // good
    }

    Thread.sleep(1001);

    try {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      throw new AssertionError("Should have failed!");
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(IOException.class);
      // good
    }

    try {
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();
      throw new AssertionError("Should have failed!");
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(CallNotPermittedException.class);
      // good
    }
  }

}
