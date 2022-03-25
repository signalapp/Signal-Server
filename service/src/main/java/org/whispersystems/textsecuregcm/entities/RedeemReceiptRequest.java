/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotEmpty;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public class RedeemReceiptRequest {

  private final byte[] receiptCredentialPresentation;
  private final boolean visible;
  private final boolean primary;

  @JsonCreator
  public RedeemReceiptRequest(
      @JsonProperty("receiptCredentialPresentation") byte[] receiptCredentialPresentation,
      @JsonProperty("visible") boolean visible,
      @JsonProperty("primary") boolean primary) {
    this.receiptCredentialPresentation = receiptCredentialPresentation;
    this.visible = visible;
    this.primary = primary;
  }

  @NotEmpty
  public byte[] getReceiptCredentialPresentation() {
    return receiptCredentialPresentation;
  }

  public boolean isVisible() {
    return visible;
  }

  public boolean isPrimary() {
    return primary;
  }
}
