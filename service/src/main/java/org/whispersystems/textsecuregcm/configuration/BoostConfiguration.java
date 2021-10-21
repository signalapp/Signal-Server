/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class BoostConfiguration {

  private final Map<String, List<BigDecimal>> currencies;

  @JsonCreator
  public BoostConfiguration(
      @JsonProperty("currencies") final Map<String, List<BigDecimal>> currencies) {
    this.currencies = currencies;
  }

  @Valid
  @NotNull
  public Map<@NotEmpty String, @Valid @ExactlySize(6) List<@DecimalMin("0.01") @NotNull BigDecimal>> getCurrencies() {
    return currencies;
  }
}
