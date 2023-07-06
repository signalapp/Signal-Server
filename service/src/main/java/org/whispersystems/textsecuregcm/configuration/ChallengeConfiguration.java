/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.time.Duration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;

public record ChallengeConfiguration(SecretBytes blindingSecret, Duration tokenTtl) {
}
