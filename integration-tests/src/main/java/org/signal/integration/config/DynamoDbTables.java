/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration.config;

import javax.validation.constraints.NotBlank;

public record DynamoDbTables(@NotBlank String registrationRecovery,
                             @NotBlank String verificationSessions) {
}
