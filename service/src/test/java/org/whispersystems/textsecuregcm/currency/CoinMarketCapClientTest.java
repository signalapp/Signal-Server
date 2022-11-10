package org.whispersystems.textsecuregcm.currency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CoinMarketCapClientTest {

  private static final String RESPONSE_JSON = """
          {
            "status": {
              "timestamp": "2022-11-09T17:15:06.356Z",
              "error_code": 0,
              "error_message": null,
              "elapsed": 41,
              "credit_count": 1,
              "notice": null
            },
            "data": {
              "id": 7878,
              "symbol": "MOB",
              "name": "MobileCoin",
              "amount": 1,
              "last_updated": "2022-11-09T17:14:00.000Z",
              "quote": {
                "USD": {
                  "price": 0.6625319895827952,
                  "last_updated": "2022-11-09T17:14:00.000Z"
                }
              }
            }
          }
      """;

  @Test
  void parseResponse() throws JsonProcessingException {
    final CoinMarketCapClient.CoinMarketCapResponse parsedResponse = CoinMarketCapClient.parseResponse(RESPONSE_JSON);

    assertEquals(7878, parsedResponse.priceConversionResponse().id());
    assertEquals("MOB", parsedResponse.priceConversionResponse().symbol());

    final Map<String, CoinMarketCapClient.PriceConversionQuote> quote =
        parsedResponse.priceConversionResponse().quote();

    assertEquals(1, quote.size());
    assertEquals(new BigDecimal("0.6625319895827952"), quote.get("USD").price());
  }

  @Test
  void extractConversionRate() throws IOException {
    final CoinMarketCapClient.CoinMarketCapResponse parsedResponse = CoinMarketCapClient.parseResponse(RESPONSE_JSON);

    assertEquals(new BigDecimal("0.6625319895827952"), CoinMarketCapClient.extractConversionRate(parsedResponse, "USD"));
    assertThrows(IOException.class, () -> CoinMarketCapClient.extractConversionRate(parsedResponse, "CAD"));
  }
}
