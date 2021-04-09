/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.http;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class FaultTolerantHttpClientTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

  @Test
  public void testSimpleGet() {
    wireMockRule.stubFor(get(urlEqualTo("/ping"))
                             .willReturn(aResponse()
                                             .withHeader("Content-Type", "text/plain")
                                             .withBody("Pong!")));


    FaultTolerantHttpClient client = FaultTolerantHttpClient.newBuilder()
                                                            .withCircuitBreaker(new CircuitBreakerConfiguration())
                                                            .withRetry(new RetryConfiguration())
                                                            .withExecutor(Executors.newSingleThreadExecutor())
                                                            .withName("test")
                                                            .withVersion(HttpClient.Version.HTTP_2)
                                                            .build();

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(URI.create("http://localhost:" + wireMockRule.port() + "/ping"))
                                     .GET()
                                     .build();

    HttpResponse<String> response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("Pong!");

    verify(1, getRequestedFor(urlEqualTo("/ping")));
  }

  @Test
  public void testRetryGet() {
    wireMockRule.stubFor(get(urlEqualTo("/failure"))
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "text/plain")
                                             .withBody("Pong!")));

    FaultTolerantHttpClient client = FaultTolerantHttpClient.newBuilder()
                                                            .withCircuitBreaker(new CircuitBreakerConfiguration())
                                                            .withRetry(new RetryConfiguration())
                                                            .withExecutor(Executors.newSingleThreadExecutor())
                                                            .withName("test")
                                                            .withVersion(HttpClient.Version.HTTP_2)
                                                            .build();

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(URI.create("http://localhost:" + wireMockRule.port() + "/failure"))
                                     .GET()
                                     .build();

    HttpResponse<String> response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).join();

    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body()).isEqualTo("Pong!");

    verify(3, getRequestedFor(urlEqualTo("/failure")));
  }

  @Test
  public void testNetworkFailureCircuitBreaker() throws InterruptedException {
    CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();
    circuitBreakerConfiguration.setRingBufferSizeInClosedState(2);
    circuitBreakerConfiguration.setRingBufferSizeInHalfOpenState(1);
    circuitBreakerConfiguration.setFailureRateThreshold(50);
    circuitBreakerConfiguration.setWaitDurationInOpenStateInSeconds(1);

    FaultTolerantHttpClient client = FaultTolerantHttpClient.newBuilder()
                                                            .withCircuitBreaker(circuitBreakerConfiguration)
                                                            .withRetry(new RetryConfiguration())
                                                            .withExecutor(Executors.newSingleThreadExecutor())
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
