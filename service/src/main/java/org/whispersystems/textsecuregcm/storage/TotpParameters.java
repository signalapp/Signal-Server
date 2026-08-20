/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;

public record TotpParameters(@JsonProperty("alg")
                             String algorithm,

                             @JsonProperty("len")
                             int passwordLength,

                             @JsonProperty("step")
                             Duration timeStep) {
}
