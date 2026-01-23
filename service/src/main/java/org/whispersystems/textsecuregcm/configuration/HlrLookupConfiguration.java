/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import javax.annotation.Nullable;

public record HlrLookupConfiguration(SecretString apiKey,
                                     SecretString apiSecret,
                                     @Nullable String circuitBreakerConfigurationName,
                                     @Nullable String retryConfigurationName) {
}
