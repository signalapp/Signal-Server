package org.whispersystems.textsecuregcm.currency;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class CoinMarketCapClient {

  private final HttpClient httpClient;
  private final String apiKey;
  private final Map<String, Integer> currencyIdsBySymbol;

  private static final Logger logger = LoggerFactory.getLogger(CoinMarketCapClient.class);

  record CoinMarketCapResponse(@JsonProperty("data") PriceConversionResponse priceConversionResponse) {};

  record PriceConversionResponse(int id, String symbol, Map<String, PriceConversionQuote> quote) {};

  record PriceConversionQuote(BigDecimal price) {};

  public CoinMarketCapClient(final HttpClient httpClient, final String apiKey, final Map<String, Integer> currencyIdsBySymbol) {
    this.httpClient = httpClient;
    this.apiKey = apiKey;
    this.currencyIdsBySymbol = currencyIdsBySymbol;
  }

  public BigDecimal getSpotPrice(final String currency, final String base) throws IOException {
    if (!currencyIdsBySymbol.containsKey(currency)) {
      throw new IllegalArgumentException("No currency ID found for " + currency);
    }

    final URI quoteUri = URI.create(
        String.format("https://pro-api.coinmarketcap.com/v2/tools/price-conversion?amount=1&id=%d&convert=%s",
            currencyIdsBySymbol.get(currency), base));

    try {
      final HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
              .GET()
              .uri(quoteUri)
              .header("X-CMC_PRO_API_KEY", apiKey)
              .build(),
          HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        logger.warn("CoinMarketCapRequest failed with response: {}", response);
        throw new IOException("CoinMarketCap request failed with status code " + response.statusCode());
      }

      return extractConversionRate(parseResponse(response.body()), base);
    } catch (final InterruptedException e) {
      throw new IOException("Interrupted while waiting for a response", e);
    }
  }

  @VisibleForTesting
  static CoinMarketCapResponse parseResponse(final String responseJson) throws JsonProcessingException {
    return SystemMapper.getMapper().readValue(responseJson, CoinMarketCapResponse.class);
  }

  @VisibleForTesting
  static BigDecimal extractConversionRate(final CoinMarketCapResponse response, final String destinationCurrency)
      throws IOException {
    if (!response.priceConversionResponse().quote.containsKey(destinationCurrency)) {
      throw new IOException("Response does not contain conversion rate for " + destinationCurrency);
    }

    return response.priceConversionResponse().quote.get(destinationCurrency).price();
  }
}
