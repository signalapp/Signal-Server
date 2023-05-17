/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import io.dropwizard.util.Strings;
import io.dropwizard.validation.ValidationMethod;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record GcpAttachmentsConfiguration(@NotBlank String domain,
                                          @NotBlank String email,
                                          @Min(1) int maxSizeInBytes,
                                          String pathPrefix,
                                          @NotNull SecretString rsaSigningKey) {
  @SuppressWarnings("unused")
  @ValidationMethod(message = "pathPrefix must be empty or start with /")
  public boolean isPathPrefixValid() {
    return Strings.isNullOrEmpty(pathPrefix) || pathPrefix.startsWith("/");
  }
}
