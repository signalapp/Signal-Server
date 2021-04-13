/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.gcm.server;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static junit.framework.TestCase.assertTrue;
import static org.whispersystems.gcm.server.util.FixtureHelpers.fixture;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class SimultaneousSenderTest {

  @Rule
  public WireMockRule wireMock = new WireMockRule(WireMockConfiguration.options().dynamicPort().dynamicHttpsPort());

  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void testSimultaneousSuccess() throws TimeoutException, InterruptedException, ExecutionException, JsonProcessingException {
    stubFor(post(urlPathEqualTo("/gcm/send"))
                .willReturn(aResponse()
                                .withStatus(200)
                                .withBody(fixture("fixtures/response-success.json"))));

    Sender                          sender  = new Sender("foobarbaz", mapper, 2, "http://localhost:" + wireMock.port() + "/gcm/send");
    List<CompletableFuture<Result>> results = new LinkedList<>();

    for (int i=0;i<1000;i++) {
      results.add(sender.send(Message.newBuilder().withDestination("1").build()));
    }

    for (CompletableFuture<Result> future : results) {
      Result result = future.get(60, TimeUnit.SECONDS);

      if (!result.isSuccess()) {
        throw new AssertionError(result.getError());
      }
    }
  }

  @Test
  @Ignore
  public void testSimultaneousFailure() throws TimeoutException, InterruptedException {
    stubFor(post(urlPathEqualTo("/gcm/send"))
                .willReturn(aResponse()
                                .withStatus(503)));

    Sender                         sender   = new Sender("foobarbaz", mapper, 2, "http://localhost:" + wireMock.port() + "/gcm/send");
    List<CompletableFuture<Result>> futures = new LinkedList<>();

    for (int i=0;i<1000;i++) {
      futures.add(sender.send(Message.newBuilder().withDestination("1").build()));
    }

    for (CompletableFuture<Result> future : futures) {
      try {
        Result result = future.get(60, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        assertTrue(e.getCause().toString(), e.getCause() instanceof ServerFailedException);
      }
    }
  }
}
