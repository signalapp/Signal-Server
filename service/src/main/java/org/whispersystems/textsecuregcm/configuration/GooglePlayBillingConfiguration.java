/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

/**
 * @param credentialsJson  Service account credentials for Play Billing API
 * @param packageName      The app package name
 * @param applicationName  The app application name
 * @param productIdToLevel A map of productIds offered in the play billing subscription catalog to their corresponding
 *                         signal subscription level
 */
public record GooglePlayBillingConfiguration(
    @NotNull SecretString credentialsJson,
    @NotNull String packageName,
    @NotBlank String applicationName,
    @NotNull Map<String, Long> productIdToLevel) {}
