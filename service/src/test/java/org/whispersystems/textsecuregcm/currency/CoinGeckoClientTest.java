package org.whispersystems.textsecuregcm.currency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CoinGeckoClientTest {

  private static final String RESPONSE_JSON = """
          {
            "mobilecoin": {
              "usd": 0.226212
            }
          }
      """;

  @Test
  void parseResponse() throws JsonProcessingException {
    final Map<String, Map<String, BigDecimal>> parsedResponse = CoinGeckoClient.parseResponse(RESPONSE_JSON);

    assertTrue(parsedResponse.containsKey("mobilecoin"));

    assertEquals(1, parsedResponse.get("mobilecoin").size());
    assertEquals(new BigDecimal("0.226212"), parsedResponse.get("mobilecoin").get("usd"));
  }

  @Test
  void extractConversionRate() throws IOException {
    final Map<String, Map<String, BigDecimal>> parsedResponse = CoinGeckoClient.parseResponse(RESPONSE_JSON);

    assertEquals(new BigDecimal("0.226212"), CoinGeckoClient.extractConversionRate(parsedResponse.get("mobilecoin"), "usd"));
    assertThrows(IOException.class, () -> CoinGeckoClient.extractConversionRate(parsedResponse.get("mobilecoin"), "CAD"));
  }
}
