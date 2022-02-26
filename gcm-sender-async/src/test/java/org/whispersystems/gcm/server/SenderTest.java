/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.gcm.server;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.whispersystems.gcm.server.util.FixtureHelpers.fixture;
import static org.whispersystems.gcm.server.util.JsonHelpers.jsonFixture;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SenderTest {

  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  void testSuccess() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    wireMock.stubFor(any(anyUrl())
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(fixture("fixtures/response-success.json"))));


    Sender                    sender = new Sender("foobarbaz", mapper, 10, "http://localhost:" + wireMock.getPort() + "/gcm/send");
    CompletableFuture<Result> future = sender.send(Message.newBuilder().withDestination("1").build());

    Result result = future.get(10, TimeUnit.SECONDS);

    assertTrue(result.isSuccess());
    assertFalse(result.isThrottled());
    assertFalse(result.isUnregistered());
    assertEquals(result.getMessageId(), "1:08");
    assertNull(result.getError());
    assertNull(result.getCanonicalRegistrationId());

    wireMock.verify(1, postRequestedFor(urlEqualTo("/gcm/send"))
        .withHeader("Authorization", equalTo("key=foobarbaz"))
        .withHeader("Content-Type", equalTo("application/json"))
        .withRequestBody(equalTo(jsonFixture("fixtures/message-minimal.json"))));
  }

  @Test
  void testBadApiKey() throws InterruptedException, TimeoutException {
    wireMock.stubFor(any(anyUrl())
        .willReturn(aResponse()
            .withStatus(401)));

    Sender                    sender = new Sender("foobar", mapper, 10, "http://localhost:" + wireMock.getPort() + "/gcm/send");
    CompletableFuture<Result> future = sender.send(Message.newBuilder().withDestination("1").build());

    try {
      future.get(10, TimeUnit.SECONDS);
      throw new AssertionError();
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof AuthenticationFailedException);
    }

    wireMock.verify(1, anyRequestedFor(anyUrl()));
  }

  @Test
  void testBadRequest() throws TimeoutException, InterruptedException {
    wireMock.stubFor(any(anyUrl())
        .willReturn(aResponse()
            .withStatus(400)));

    Sender                    sender = new Sender("foobarbaz", mapper, 10, "http://localhost:" + wireMock.getPort() + "/gcm/send");
    CompletableFuture<Result> future = sender.send(Message.newBuilder().withDestination("1").build());

    try {
      future.get(10, TimeUnit.SECONDS);
      throw new AssertionError();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof InvalidRequestException);
    }

    wireMock.verify(1, anyRequestedFor(anyUrl()));
  }

  @Test
  void testServerError() throws TimeoutException, InterruptedException {
    wireMock.stubFor(any(anyUrl())
        .willReturn(aResponse()
            .withStatus(503)));

    Sender                    sender = new Sender("foobarbaz", mapper, 3, "http://localhost:" + wireMock.getPort() + "/gcm/send");
    CompletableFuture<Result> future = sender.send(Message.newBuilder().withDestination("1").build());

    try {
      future.get(10, TimeUnit.SECONDS);
      throw new AssertionError();
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof ServerFailedException);
    }

    wireMock.verify(3, anyRequestedFor(anyUrl()));
  }

  @Test
  void testServerErrorRecovery() throws InterruptedException, ExecutionException, TimeoutException {

    wireMock.stubFor(any(anyUrl()).willReturn(aResponse().withStatus(503)));

    Sender                    sender = new Sender("foobarbaz", mapper, 4, "http://localhost:" + wireMock.getPort() + "/gcm/send");
    CompletableFuture<Result> future = sender.send(Message.newBuilder().withDestination("1").build());

    // up to three failures can happen, with 100ms exponential backoff
    // if we end up using the fourth, and final try, it would be after ~700 ms
    CompletableFuture.delayedExecutor(300, TimeUnit.MILLISECONDS).execute(() ->
        wireMock.stubFor(any(anyUrl())
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(fixture("fixtures/response-success.json"))))
    );

    Result result = future.get(10, TimeUnit.SECONDS);

    wireMock.verify(new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN, 1), anyRequestedFor(anyUrl()));
    assertTrue(result.isSuccess());
    assertFalse(result.isThrottled());
    assertFalse(result.isUnregistered());
    assertEquals(result.getMessageId(), "1:08");
    assertNull(result.getError());
    assertNull(result.getCanonicalRegistrationId());
  }

  @Test
  void testNetworkError() throws TimeoutException, InterruptedException {

    wireMock.stubFor(any(anyUrl())
        .willReturn(ok()));

    Sender sender = new Sender("foobarbaz", mapper ,2, "http://localhost:" + wireMock.getPort() + "/gcm/send");

    wireMock.getRuntimeInfo().getWireMock().shutdown();

    CompletableFuture<Result> future = sender.send(Message.newBuilder().withDestination("1").build());

    try {
      future.get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IOException);
    }
  }

  @Test
  void testNotRegistered() throws InterruptedException, ExecutionException, TimeoutException {

    wireMock.stubFor(any(anyUrl()).willReturn(aResponse().withStatus(200)
        .withBody(fixture("fixtures/response-not-registered.json"))));

    Sender                    sender = new Sender("foobarbaz", mapper,2, "http://localhost:" + wireMock.getPort() + "/gcm/send");
    CompletableFuture<Result> future = sender.send(Message.newBuilder()
                                                         .withDestination("2")
                                                         .withDataPart("message", "new message!")
                                                         .build());

    Result result = future.get(10, TimeUnit.SECONDS);

    assertFalse(result.isSuccess());
    assertTrue(result.isUnregistered());
    assertFalse(result.isThrottled());
    assertEquals(result.getError(), "NotRegistered");
  }
}
