/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import java.time.Duration;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record ArtServiceConfiguration(@ExactlySize(32) SecretBytes userAuthenticationTokenSharedSecret,
                                      @NotNull SecretBytes userAuthenticationTokenUserIdSecret,
                                      @NotNull Duration tokenExpiration) {
  public ArtServiceConfiguration {
    tokenExpiration = firstNonNull(tokenExpiration, Duration.ofDays(1));
  }
}
