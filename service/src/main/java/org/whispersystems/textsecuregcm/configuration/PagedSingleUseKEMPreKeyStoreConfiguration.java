/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import javax.annotation.Nullable;
import java.net.URI;

public record PagedSingleUseKEMPreKeyStoreConfiguration(
    @NotBlank String bucket,
    @NotBlank String region,
    @Nullable URI endpointOverride) {
}
