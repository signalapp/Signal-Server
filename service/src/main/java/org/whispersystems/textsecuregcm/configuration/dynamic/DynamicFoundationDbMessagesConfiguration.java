/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.PositiveOrZero;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStore;

public record DynamicFoundationDbMessagesConfiguration(@PositiveOrZero @Max(FoundationDbMessageStore.MAX_EPOCHS - 1) int activeEpoch) {
}
