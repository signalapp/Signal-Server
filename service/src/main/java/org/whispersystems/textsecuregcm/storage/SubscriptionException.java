/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

public class SubscriptionException extends Exception {
  public SubscriptionException(String message, Exception cause) {
    super(message, cause);
  }

  public static class NotFound extends SubscriptionException {
    public NotFound() {
      super(null, null);
    }

    public NotFound(Exception cause) {
      super(null, cause);
    }
  }

  public static class Forbidden extends SubscriptionException {
    public Forbidden(final String message) {
      super(message, null);
    }
  }

  public static class InvalidArguments extends SubscriptionException {
    public InvalidArguments(final String message, final Exception cause) {
      super(message, cause);
    }
  }

  public static class InvalidLevel extends InvalidArguments {
    public InvalidLevel() {
      super(null, null);
    }
  }

  public static class PaymentRequiresAction extends InvalidArguments {
    public PaymentRequiresAction() {
      super(null, null);
    }
  }

  public static class ProcessorConflict extends SubscriptionException {
    public ProcessorConflict(final String message) {
      super(message, null);
    }
  }
}
