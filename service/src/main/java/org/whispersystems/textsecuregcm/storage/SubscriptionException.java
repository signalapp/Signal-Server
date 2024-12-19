/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.subscriptions.ChargeFailure;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;

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

    public InvalidArguments(final String message) {
      this(message, null);
    }
  }

  public static class InvalidLevel extends InvalidArguments {

    public InvalidLevel() {
      super(null, null);
    }
  }

  public static class InvalidAmount extends InvalidArguments {
    private String errorCode;

    public InvalidAmount(String errorCode) {
      super(null, null);
      this.errorCode = errorCode;
    }

    public String getErrorCode() {
      return errorCode;
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

  public static class ChargeFailurePaymentRequired extends SubscriptionException {

    private final ChargeFailure chargeFailure;

    public ChargeFailurePaymentRequired(final ChargeFailure chargeFailure) {
      super(null, null);
      this.chargeFailure = chargeFailure;
    }

    public ChargeFailure getChargeFailure() {
      return chargeFailure;
    }
  }

  public static class ProcessorException extends SubscriptionException {

    private final PaymentProvider processor;
    private final ChargeFailure chargeFailure;

    public ProcessorException(final PaymentProvider processor, final ChargeFailure chargeFailure) {
      super(null, null);
      this.processor = processor;
      this.chargeFailure = chargeFailure;
    }

    public PaymentProvider getProcessor() {
      return processor;
    }

    public ChargeFailure getChargeFailure() {
      return chargeFailure;
    }
  }

  /**
   * Attempted to retrieve a receipt for a subscription that hasn't yet been charged or the invoice is in the open
   * state
   */
  public static class ReceiptRequestedForOpenPayment extends SubscriptionException {

    public ReceiptRequestedForOpenPayment() {
      super(null, null);
    }
  }

  public static class ProcessorConflict extends SubscriptionException {
    public ProcessorConflict() {
      super(null, null);
    }

    public ProcessorConflict(final String message) {
      super(null, message);
    }
  }
}
