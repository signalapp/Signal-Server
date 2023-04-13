/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration.config;

public record DynamoDbTables(String registrationRecovery,
                             String verificationSessions) {
}
