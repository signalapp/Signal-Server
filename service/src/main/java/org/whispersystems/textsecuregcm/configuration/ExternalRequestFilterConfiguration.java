/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Set;
import org.whispersystems.textsecuregcm.util.InetAddressRange;

public record ExternalRequestFilterConfiguration(@Valid @NotNull Set<@NotNull String> paths,
                                                 @Valid @NotNull Set<@NotNull InetAddressRange> permittedInternalRanges,
                                                 @Valid @NotNull Set<@NotNull String> grpcMethods) {
}
