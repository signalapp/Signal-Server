/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

public enum PaymentMethod {
  UNKNOWN,
  /**
   * A credit card or debit card, including those from Apple Pay and Google Pay
   */
  CARD,
  /**
   * A PayPal account
   */
  PAYPAL,
  /**
   * A SEPA debit account
   */
  SEPA_DEBIT,
  /**
   * An iDEAL account
   */
  IDEAL,
  GOOGLE_PLAY_BILLING,
  APPLE_APP_STORE;


  public org.signal.chat.purchase.PaymentMethod toProtoPaymentMethod() {
    return switch (this) {
      case CARD -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_CARD;
      case SEPA_DEBIT -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_SEPA_DEBIT;
      case IDEAL -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_IDEAL;
      case PAYPAL -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_PAYPAL;
      case GOOGLE_PLAY_BILLING -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_GOOGLE_PLAY_BILLING;
      case APPLE_APP_STORE -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_APPLE_APP_STORE;
      case UNKNOWN -> org.signal.chat.purchase.PaymentMethod.PAYMENT_METHOD_UNKNOWN;
    };
  }
}
