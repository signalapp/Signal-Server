/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.InetAddressRange;

public record ExternalRequestFilterConfiguration(@Valid @NotNull Set<@NotNull String> paths,
                                                 @Valid @NotNull Set<@NotNull InetAddressRange> permittedInternalRanges,
                                                 @Valid @NotNull Set<@NotNull String> grpcMethods) {
}
