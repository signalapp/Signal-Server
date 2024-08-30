/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum SubscriptionStatus {
  /**
   * The subscription is in good standing and the most recent payment was successful.
   */
  ACTIVE("active"),

  /**
   * Payment failed when creating the subscription, or the subscription’s start date is in the future.
   */
  INCOMPLETE("incomplete"),

  /**
   * Payment on the latest renewal failed but there are processor retries left, or payment wasn't attempted.
   */
  PAST_DUE("past_due"),

  /**
   * The subscription has been canceled.
   */
  CANCELED("canceled"),

  /**
   * The latest renewal hasn't been paid but the subscription remains in place.
   */
  UNPAID("unpaid"),

  /**
   * The status from the downstream processor is unknown.
   */
  UNKNOWN("unknown");


  private final String apiValue;

  SubscriptionStatus(String apiValue) {
    this.apiValue = apiValue;
  }

  public static SubscriptionStatus forApiValue(String status) {
    return switch (status) {
      case "active" -> ACTIVE;
      case "canceled", "incomplete_expired" -> CANCELED;
      case "unpaid" -> UNPAID;
      case "past_due" -> PAST_DUE;
      case "incomplete" -> INCOMPLETE;

      case "trialing" -> {
        final Logger logger = LoggerFactory.getLogger(CustomerAwareSubscriptionPaymentProcessor.class);
        logger.error("Subscription has status that should never happen: {}", status);

        yield UNKNOWN;
      }
      default -> {
        final Logger logger = LoggerFactory.getLogger(CustomerAwareSubscriptionPaymentProcessor.class);
        logger.error("Subscription has unknown status: {}", status);

        yield UNKNOWN;
      }
    };
  }

  public String getApiValue() {
    return apiValue;
  }
}
