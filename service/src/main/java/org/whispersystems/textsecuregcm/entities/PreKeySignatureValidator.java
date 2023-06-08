/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.Collection;
import org.signal.libsignal.protocol.IdentityKey;

public abstract class PreKeySignatureValidator {
  public static final Counter INVALID_SIGNATURE_COUNTER =
      Metrics.counter(name(PreKeySignatureValidator.class, "invalidPreKeySignature"));

  public static boolean validatePreKeySignatures(final IdentityKey identityKey, final Collection<SignedPreKey> spks) {
    try {
      final boolean success = spks.stream()
          .allMatch(spk -> identityKey.getPublicKey().verifySignature(spk.getPublicKey(), spk.getSignature()));

      if (!success) {
        INVALID_SIGNATURE_COUNTER.increment();
      }

      return success;
    } catch (final IllegalArgumentException e) {
      INVALID_SIGNATURE_COUNTER.increment();
      return false;
    }
  }
}
