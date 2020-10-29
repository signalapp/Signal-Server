/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.RegEx;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.Objects;

public class PaymentAddress {

  @JsonProperty
  @NotEmpty
  @Size(max = 256)
  private String address;

  @JsonProperty
  @NotEmpty
  @Size(min = 88, max = 88)
  private String signature;

  public PaymentAddress() {}

  public PaymentAddress(String address, String signature) {
    this.address   = address;
    this.signature = signature;
  }

  public String getSignature() {
    return signature;
  }

  public String getAddress() {
    return address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)                               return true;
    if (o == null || getClass() != o.getClass()) return false;

    PaymentAddress that = (PaymentAddress) o;
    return Objects.equals(address, that.address) && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, signature);
  }
}
