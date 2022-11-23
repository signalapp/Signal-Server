/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

public enum PaymentMethod {
  /**
   * A credit card or debit card, including those from Apple Pay and Google Pay
   */
  CARD,
  /**
   * A PayPal account
   */
  PAYPAL,
}
