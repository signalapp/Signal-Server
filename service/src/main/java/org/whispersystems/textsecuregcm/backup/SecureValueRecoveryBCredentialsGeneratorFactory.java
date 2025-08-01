/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecoveryConfiguration;
import java.time.Clock;

public class SecureValueRecoveryBCredentialsGeneratorFactory {
  private SecureValueRecoveryBCredentialsGeneratorFactory() {}


  @VisibleForTesting
  static ExternalServiceCredentialsGenerator svrbCredentialsGenerator(
      final SecureValueRecoveryConfiguration cfg,
      final Clock clock) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret().value())
        .prependUsername(false)
        .withDerivedUsernameTruncateLength(16)
        .withClock(clock)
        .build();
  }

  public static ExternalServiceCredentialsGenerator svrbCredentialsGenerator(final SecureValueRecoveryConfiguration cfg) {
    return svrbCredentialsGenerator(cfg, Clock.systemUTC());
  }
}
