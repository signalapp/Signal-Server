/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import org.whispersystems.textsecuregcm.currency.CoinGeckoClient;
import org.whispersystems.textsecuregcm.currency.FixerClient;
import java.net.http.HttpClient;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = PaymentsServiceClientsConfiguration.class)
public interface PaymentsServiceClientsFactory extends Discoverable {

  FixerClient buildFixerClient(final HttpClient httpClient);

  CoinGeckoClient buildCoinGeckoClient(HttpClient httpClient);
}
