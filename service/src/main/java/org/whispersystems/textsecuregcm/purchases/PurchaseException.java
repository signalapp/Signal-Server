/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

import java.util.Optional;
import javax.annotation.Nullable;

public class PurchaseException extends Exception {

  private @Nullable String errorDetail;

  public PurchaseException(Exception cause) {
    this(cause, null);
  }

  PurchaseException(Exception cause, String errorDetail) {
    super(cause);
    this.errorDetail = errorDetail;
  }

  /**
   * @return An error message suitable to include in a client response
   */
  public Optional<String> errorDetail() {
    return Optional.ofNullable(errorDetail);
  }

}
