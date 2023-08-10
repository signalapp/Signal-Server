/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import com.google.api.Http;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShortCodeExpanderTest {

  @Test
  public void testUriResolution() throws IOException, InterruptedException {
    final HttpClient httpClient = mock(HttpClient.class);
    final ShortCodeExpander expander = new ShortCodeExpander(httpClient, "https://www.example.org/shortener/");
    when(httpClient
        .send(argThat(req -> req.uri().toString().equals("https://www.example.org/shortener/shorturl")), any()))
        .thenReturn(new FakeResponse(200, "longurl"));
    assertThat(expander.retrieve("shorturl").get()).isEqualTo("longurl");
  }

  private record FakeResponse(int statusCode, String body) implements HttpResponse<Object> {

    @Override
    public HttpRequest request() {
      return null;
    }

    @Override
    public Optional<HttpResponse<Object>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public HttpHeaders headers() {
      return null;
    }

    @Override
    public Optional<SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return null;
    }

    @Override
    public HttpClient.Version version() {
      return null;
    }

  }

}
