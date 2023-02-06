/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

public class PaymentsServiceConfiguration {

  @NotEmpty
  @JsonProperty
  private String userAuthenticationTokenSharedSecret;

  @NotBlank
  @JsonProperty
  private String coinMarketCapApiKey;

  @JsonProperty
  @NotEmpty
  private Map<@NotBlank String, Integer> coinMarketCapCurrencyIds;

  @NotEmpty
  @JsonProperty
  private String fixerApiKey;

  @NotEmpty
  @JsonProperty
  private List<String> paymentCurrencies;

  public byte[] getUserAuthenticationTokenSharedSecret() {
    return HexFormat.of().parseHex(userAuthenticationTokenSharedSecret);
  }

  public String getCoinMarketCapApiKey() {
    return coinMarketCapApiKey;
  }

  public Map<String, Integer> getCoinMarketCapCurrencyIds() {
    return coinMarketCapCurrencyIds;
  }

  public String getFixerApiKey() {
    return fixerApiKey;
  }

  public List<String> getPaymentCurrencies() {
    return paymentCurrencies;
  }
}
