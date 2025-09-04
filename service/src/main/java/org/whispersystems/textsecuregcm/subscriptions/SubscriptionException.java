/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import java.util.Optional;
import javax.annotation.Nullable;

public class SubscriptionException extends Exception {

  private @Nullable String errorDetail;

  public SubscriptionException(Exception cause) {
    this(cause, null);
  }

  SubscriptionException(Exception cause, String errorDetail) {
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
