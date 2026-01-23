/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony.hlrlookup;

import javax.annotation.Nullable;
import java.util.List;

record HlrLookupResponse(@Nullable List<HlrLookupResult> results,
                         @Nullable String error,
                         @Nullable String message) {
}
