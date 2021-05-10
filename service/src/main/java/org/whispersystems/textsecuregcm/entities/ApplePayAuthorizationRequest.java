/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

public class ApplePayAuthorizationRequest {

  private String currency;
  private long amount;

  @JsonProperty
  @NotEmpty
  @Size(min=3, max=3)
  @Pattern(regexp="[a-z]{3}")
  public String getCurrency() {
    return currency;
  }

  public void setCurrency(final String currency) {
    this.currency = currency;
  }

  @JsonProperty
  @Min(0)
  public long getAmount() {
    return amount;
  }

  @VisibleForTesting
  public void setAmount(final long amount) {
    this.amount = amount;
  }
}
