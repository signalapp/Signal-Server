/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record CallQualitySurveyConfiguration (@Valid @NotNull PubSubPublisherFactory pubSubPublisher) {
}
