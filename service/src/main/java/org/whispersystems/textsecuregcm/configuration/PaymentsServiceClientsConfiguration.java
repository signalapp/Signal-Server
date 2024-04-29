/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.net.http.HttpClient;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import org.whispersystems.textsecuregcm.currency.CoinMarketCapClient;
import org.whispersystems.textsecuregcm.currency.FixerClient;

@JsonTypeName("default")
public record PaymentsServiceClientsConfiguration(@NotNull SecretString coinMarketCapApiKey,
                                                  @NotNull SecretString fixerApiKey,
                                                  @NotEmpty Map<@NotBlank String, Integer> coinMarketCapCurrencyIds) implements
    PaymentsServiceClientsFactory {

  @Override
  public FixerClient buildFixerClient(final HttpClient httpClient) {
    return new FixerClient(httpClient, fixerApiKey.value());
  }

  @Override
  public CoinMarketCapClient buildCoinMarketCapClient(final HttpClient httpClient) {
    return new CoinMarketCapClient(httpClient, coinMarketCapApiKey.value(), coinMarketCapCurrencyIds);
  }
}
