/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony.hlrlookup;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
record HlrLookupResult(String error,
                       String originalNetwork,
                       NetworkDetails originalNetworkDetails,
                       String currentNetwork,
                       NetworkDetails currentNetworkDetails,
                       String telephoneNumberType) {
}
