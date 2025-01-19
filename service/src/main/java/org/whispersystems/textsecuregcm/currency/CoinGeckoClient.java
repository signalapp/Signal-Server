/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.currency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class CoinGeckoClient {

  private final HttpClient httpClient;
  private final String apiKey;
  private final Map<String, String> currencyIdsBySymbol;

  private static final Logger logger = LoggerFactory.getLogger(CoinGeckoClient.class);

  private static final TypeReference<Map<String, Map<String, BigDecimal>>> RESPONSE_TYPE = new TypeReference<>() {};

  public CoinGeckoClient(final HttpClient httpClient, final String apiKey, final Map<String, String> currencyIdsBySymbol) {
    this.httpClient = httpClient;
    this.apiKey = apiKey;
    this.currencyIdsBySymbol = currencyIdsBySymbol;
  }

  public BigDecimal getSpotPrice(final String currency, final String base) throws IOException {
    if (!currencyIdsBySymbol.containsKey(currency)) {
      throw new IllegalArgumentException("No currency ID found for " + currency);
    }

    final URI quoteUri = URI.create(
        String.format("https://pro-api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=%s",
            currencyIdsBySymbol.get(currency), base.toLowerCase(Locale.ROOT)));

    try {
      final HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                      .GET()
                      .uri(quoteUri)
                      .header("Accept", "application/json")
                      .header("x-cg-pro-api-key", apiKey)
                      .build(),
              HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        logger.warn("CoinGecko request failed with response: {}", response);
        throw new IOException("CoinGecko request failed with status code " + response.statusCode());
      }

      return extractConversionRate(parseResponse(response.body()).get(currencyIdsBySymbol.get(currency)), base.toLowerCase(Locale.ROOT));
    } catch (final InterruptedException e) {
      throw new IOException("Interrupted while waiting for a response", e);
    }
  }

  @VisibleForTesting
  static Map<String, Map<String,BigDecimal>> parseResponse(final String responseJson) throws JsonProcessingException {
    return SystemMapper.jsonMapper().readValue(responseJson, RESPONSE_TYPE);
  }

  @VisibleForTesting
  static BigDecimal extractConversionRate(final Map<String,BigDecimal> response, final String destinationCurrency)
      throws IOException {
    if (!response.containsKey(destinationCurrency)) {
      throw new IOException("Response does not contain conversion rate for " + destinationCurrency);
    }

    return response.get(destinationCurrency);
  }
}
