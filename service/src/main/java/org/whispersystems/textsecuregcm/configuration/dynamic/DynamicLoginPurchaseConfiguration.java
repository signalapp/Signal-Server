/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration.dynamic;

/// @param enabled whether clients may exchange a one-time purchase for a Signal Login receipt credential
public record DynamicLoginPurchaseConfiguration(boolean enabled) {}
