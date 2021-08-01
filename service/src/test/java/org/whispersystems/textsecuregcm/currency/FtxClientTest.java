package org.whispersystems.textsecuregcm.currency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import org.junit.jupiter.api.Test;

public class FtxClientTest {

  @Test
  public void testGetSpotPrice() throws IOException, InterruptedException {
    HttpResponse<String> httpResponse = mock(HttpResponse.class);
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn(jsonFixture("fixtures/ftx.res.json"));

    HttpClient httpClient = mock(HttpClient.class);
    when(httpClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(httpResponse);

    FtxClient ftxClient = new FtxClient(httpClient);
    BigDecimal spotPrice = ftxClient.getSpotPrice("FOO", "BAR");
    assertThat(spotPrice).isEqualTo(new BigDecimal("0.8017"));
  }

}
