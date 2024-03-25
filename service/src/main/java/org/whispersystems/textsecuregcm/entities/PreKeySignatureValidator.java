/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;

import javax.annotation.Nullable;
import java.util.Collection;

public abstract class PreKeySignatureValidator {
  public static final String INVALID_SIGNATURE_COUNTER_NAME =
      MetricsUtil.name(PreKeySignatureValidator.class, "invalidPreKeySignature");

  public static boolean validatePreKeySignatures(final IdentityKey identityKey,
      final Collection<SignedPreKey<?>> signedPreKeys,
      @Nullable final String userAgent,
      final String context) {

    final boolean success = signedPreKeys.stream().allMatch(signedPreKey -> signedPreKey.signatureValid(identityKey));

    if (!success) {
      Metrics.counter(INVALID_SIGNATURE_COUNTER_NAME,
              Tags.of(UserAgentTagUtil.getPlatformTag(userAgent), Tag.of("context", context)))
          .increment();
    }

    return success;
  }
}
