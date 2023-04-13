/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration.config;

import org.whispersystems.textsecuregcm.configuration.DynamoDbClientConfiguration;

public record Config(String domain,
                     String rootCert,
                     DynamoDbClientConfiguration dynamoDbClientConfiguration,
                     DynamoDbTables dynamoDbTables) {
}
