/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import javax.annotation.Nullable;

public record ChargeFailure(String code, String message, @Nullable String outcomeNetworkStatus,
                            @Nullable String outcomeReason, @Nullable String outcomeType) {

}
