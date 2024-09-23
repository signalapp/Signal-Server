/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import io.micrometer.core.instrument.Metrics;
import org.apache.http.HttpStatus;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class ShortCodeExpander {
  private static final String EXPAND_COUNTER_NAME = name(ShortCodeExpander.class, "expand");

  private final HttpClient client;
  private final URI shortenerHost;

  public ShortCodeExpander(final HttpClient client, final String shortenerHost) {
    this.client = client;
    this.shortenerHost = URI.create(shortenerHost);
  }

  public Optional<String> retrieve(final String shortCode) throws IOException {
    final URI uri = shortenerHost.resolve(URLEncoder.encode(shortCode, StandardCharsets.UTF_8));
    final HttpRequest request = HttpRequest.newBuilder().uri(uri).GET().build();

    try {
      final HttpResponse<String> response = this.client.send(request, HttpResponse.BodyHandlers.ofString());
      Metrics.counter(EXPAND_COUNTER_NAME, "responseCode", Integer.toString(response.statusCode())).increment();
      return switch (response.statusCode()) {
        case HttpStatus.SC_OK -> Optional.of(response.body());
        case HttpStatus.SC_NOT_FOUND -> Optional.empty();
        default -> throw new IOException("Failed to look up shortcode");
      };
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }



}
