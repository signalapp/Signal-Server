/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public record CloudflareTurnConfiguration(@NotNull SecretString username, @NotNull SecretString password,
                                          @Valid @NotNull List<@NotBlank String> urls) {

}
