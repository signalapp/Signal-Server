/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

/// @param currency The upper-case 3-character currency specifier of the price
/// @param amount   The amount in the currency's minor unit (as defined by stripe)
public record SubscriptionPrice(String currency, long amount) {}
