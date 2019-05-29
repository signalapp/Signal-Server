package org.whispersystems.gcm.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.util.concurrent.ListenableFuture;
import com.squareup.okhttp.mockwebserver.MockResponse;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.whispersystems.gcm.server.util.FixtureHelpers.fixture;

public class SimultaneousSenderTest {

  @Rule
  public WireMockRule wireMock = new WireMockRule(8089);

  @Test
  public void testSimultaneousSuccess() throws TimeoutException, InterruptedException, ExecutionException, JsonProcessingException {
    stubFor(post(urlPathEqualTo("/gcm/send"))
                .willReturn(aResponse()
                                .withStatus(200)
                                .withBody(fixture("fixtures/response-success.json"))));

    Sender                         sender  = new Sender("foobarbaz", 2, "http://localhost:8089/gcm/send");
    List<ListenableFuture<Result>> results = new LinkedList<>();

    for (int i=0;i<1000;i++) {
      results.add(sender.send(Message.newBuilder().withDestination("1").build()));
    }

    int i=0;
    for (ListenableFuture<Result> future : results) {
      Result result = future.get(60, TimeUnit.SECONDS);
      System.out.println("Got " + (i++));

      if (!result.isSuccess()) {
        throw new AssertionError(result.getError());
      }
    }
  }

  @Test
  public void testSimultaneousFailure() throws TimeoutException, InterruptedException {
    stubFor(post(urlPathEqualTo("/gcm/send"))
                .willReturn(aResponse()
                                .withStatus(503)));

    Sender                         sender  = new Sender("foobarbaz", 2, "http://localhost:8089/gcm/send");
    List<ListenableFuture<Result>> futures = new LinkedList<>();

    for (int i=0;i<1000;i++) {
      futures.add(sender.send(Message.newBuilder().withDestination("1").build()));
    }

    for (ListenableFuture<Result> future : futures) {
      try {
        Result result = future.get(60, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof ServerFailedException);
      }
    }
  }
}
