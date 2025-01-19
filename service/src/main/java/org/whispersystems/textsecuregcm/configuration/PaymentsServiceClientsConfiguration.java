/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.net.http.HttpClient;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import org.whispersystems.textsecuregcm.currency.CoinGeckoClient;
import org.whispersystems.textsecuregcm.currency.FixerClient;

@JsonTypeName("default")
public record PaymentsServiceClientsConfiguration(@NotNull SecretString coinGeckoApiKey,
                                                  @NotNull SecretString fixerApiKey,
                                                  @NotEmpty Map<@NotBlank String, String> coinGeckoCurrencyIds) implements
    PaymentsServiceClientsFactory {

  @Override
  public FixerClient buildFixerClient(final HttpClient httpClient) {
    return new FixerClient(httpClient, fixerApiKey.value());
  }

  @Override
  public CoinGeckoClient buildCoinGeckoClient(final HttpClient httpClient) {
    return new CoinGeckoClient(httpClient, coinGeckoApiKey.value(), coinGeckoCurrencyIds);
  }
}
