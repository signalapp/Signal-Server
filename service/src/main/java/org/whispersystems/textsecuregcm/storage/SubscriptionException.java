/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

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

  public static class NotFound extends SubscriptionException {

    public NotFound() {
      super(null);
    }

    public NotFound(Exception cause) {
      super(cause);
    }
  }

  public static class Forbidden extends SubscriptionException {

    public Forbidden(final String message) {
      super(null, message);
    }
  }

  public static class InvalidArguments extends SubscriptionException {

    public InvalidArguments(final String message, final Exception cause) {
      super(cause, message);
    }
  }

  public static class InvalidLevel extends InvalidArguments {
    public InvalidLevel() {
      super(null, null);
    }
  }

  public static class PaymentRequiresAction extends InvalidArguments {
    public PaymentRequiresAction(String message) {
      super(message, null);
    }
    public PaymentRequiresAction() {
      super(null, null);
    }
  }

  public static class PaymentRequired extends SubscriptionException {
    public PaymentRequired() {
      super(null, null);
    }
    public PaymentRequired(String message) {
      super(null, message);
    }
  }

  public static class ProcessorConflict extends SubscriptionException {
    public ProcessorConflict(final String message) {
      super(null, message);
    }
  }
}
