/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class PaymentAddressList {

  @JsonProperty
  @NotNull
  @Valid
  private List<PaymentAddress> payments;

  public PaymentAddressList() {

  }

  public PaymentAddressList(List<PaymentAddress> payments) {
    this.payments = payments;
  }

  public List<PaymentAddress> getPayments() {
    return payments;
  }
}
