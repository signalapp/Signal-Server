/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.util.Collections;
import java.util.Map;
import org.whispersystems.textsecuregcm.currency.CoinMarketCapClient;
import org.whispersystems.textsecuregcm.currency.FixerClient;

@JsonTypeName("stub")
public class StubPaymentsServiceClientsFactory implements PaymentsServiceClientsFactory {

  @Override
  public FixerClient buildFixerClient(final HttpClient httpClient) {
    return new StubFixerClient();
  }

  @Override
  public CoinMarketCapClient buildCoinMarketCapClient(final HttpClient httpClient) {
    return new StubCoinMarketCapClient();
  }

  /**
   * Always returns an empty map of conversions
   */
  private static class StubFixerClient extends FixerClient {

    public StubFixerClient() {
      super(null, null);
    }

    @Override
    public Map<String, BigDecimal> getConversionsForBase(final String base) throws FixerException {
      return Collections.emptyMap();
    }
  }

  /**
   * Always returns {@code 0} for spot price checks
   */
  private static class StubCoinMarketCapClient extends CoinMarketCapClient {

    public StubCoinMarketCapClient() {
      super(null, null, null);
    }

    @Override
    public BigDecimal getSpotPrice(final String currency, final String base) {
      return BigDecimal.ZERO;
    }
  }
}
