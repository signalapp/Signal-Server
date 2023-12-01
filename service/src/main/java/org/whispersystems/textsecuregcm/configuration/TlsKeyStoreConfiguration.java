/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import javax.validation.constraints.NotNull;

public record TlsKeyStoreConfiguration(@NotNull SecretString password) {
}
