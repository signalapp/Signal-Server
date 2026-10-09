/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.attachments;

import com.google.common.annotations.VisibleForTesting;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.hibernate.validator.constraints.time.DurationMin;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record TusConfiguration(
    @ExactlySize(32) SecretBytes userAuthenticationTokenSharedSecret,
    @NotEmpty String uploadUri,
    // The TUS server enforces its own `MAX_TOKEN_AGE`, so this must be greater than or equal to it.
    // https://github.com/signalapp/tus-server/tree/main/src/index.ts
    @NotNull @DurationMin(days = 7) Duration tokenExpiration){

  @VisibleForTesting
  public static final Duration DEFAULT_TOKEN_EXPIRATION = Duration.ofDays(7);

  public TusConfiguration {
    if (tokenExpiration == null) {
      tokenExpiration = DEFAULT_TOKEN_EXPIRATION;
    }
  }
}
