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
/// @param playProductId     the Google Play Billing productId clients should purchase to obtain a Signal Login
/// @param playOptionId      the Google Play Billing optionId clients should purchase to obtain a Signal Login
/// @param appStoreProductId the App Store productId clients should purchase to obtain a Signal Login
public record LoginPurchaseConfiguration(
    @NotEmpty String playProductId,
    @NotEmpty String playOptionId,
    @NotEmpty String appStoreProductId) {}
