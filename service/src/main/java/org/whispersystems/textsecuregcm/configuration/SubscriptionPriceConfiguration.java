/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class SubscriptionPriceConfiguration {

  private final String id;
  private final BigDecimal amount;

  @JsonCreator
  public SubscriptionPriceConfiguration(
      @JsonProperty("id") @NotEmpty String id,
      @JsonProperty("amount") @NotNull @DecimalMin("0.01") BigDecimal amount) {
    this.id = id;
    this.amount = amount;
  }

  public String getId() {
    return id;
  }

  public BigDecimal getAmount() {
    return amount;
  }
}
