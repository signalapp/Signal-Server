/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration.config;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.DynamoDbClientFactory;

public record Config(@NotBlank String domain,
                     @NotBlank String rootCert,
                     @NotNull @Valid DynamoDbClientFactory dynamoDbClient,
                     @NotNull @Valid DynamoDbTables dynamoDbTables,
                     @NotBlank String prescribedRegistrationNumber,
                     @NotBlank String prescribedRegistrationCode) {
}
