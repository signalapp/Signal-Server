/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

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
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FixerClientTest {

  @Test
  public void testGetConversionsForBase() throws IOException, InterruptedException {
    HttpResponse<String> httpResponse = mock(HttpResponse.class);
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn(jsonFixture("fixtures/fixer.res.json"));

    HttpClient httpClient = mock(HttpClient.class);
    when(httpClient.send(any(HttpRequest.class), any(BodyHandler.class))).thenReturn(httpResponse);

    FixerClient fixerClient = new FixerClient(httpClient, "foo");
    Map<String, BigDecimal> conversions = fixerClient.getConversionsForBase("EUR");
    assertThat(conversions.get("CAD")).isEqualTo(new BigDecimal("1.560132"));
  }

}
