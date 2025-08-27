/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;

public record SecureStorageServiceConfiguration(@NotNull SecretBytes userAuthenticationTokenSharedSecret,
                                                @NotBlank String uri,
                                                @NotEmpty List<@NotBlank String> storageCaCertificates,
                                                @Nullable String circuitBreakerConfigurationName,
                                                @Nullable String retryConfigurationName) {
}
