/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretStringList;

public record RemoteConfigConfiguration(@NotNull SecretStringList authorizedTokens,
                                        @NotNull Set<String> authorizedUsers,
                                        @NotNull String requiredHostedDomain,
                                        @NotNull @NotEmpty List<String> audiences,
                                        @NotNull Map<String, String> globalConfig) {

}
