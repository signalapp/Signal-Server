/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;

/// Configuration for one-time Signal Login purchases
///
/// @param level             the receipt level that identifies a purchase as a Signal Login.
/// @param playProductId     the Google Play Billing productId clients should purchase to obtain a Signal Login
/// @param appStoreProductId the App Store productId clients should purchase to obtain a Signal Login
public record LoginPurchaseConfiguration(
    @Positive long level,
    @NotEmpty String playProductId,
    @NotEmpty String appStoreProductId) {}
