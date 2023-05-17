/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.Map;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretStringList;

public record RemoteConfigConfiguration(@NotNull SecretStringList authorizedTokens,
                                        @NotNull Map<String, String> globalConfig) {
}
