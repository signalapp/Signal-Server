/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;

public class RedeemReceiptRequest {

  @Schema(description = "Presentation of a ZK receipt encoded in standard padded base64", implementation = String.class)
  private final byte[] receiptCredentialPresentation;
  @Schema(description = "If true, the corresponding badge should be visible on the profile")
  private final boolean visible;
  @Schema(description = "if true, and the new badge is visible, it should be the primary badge on the profile")
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
