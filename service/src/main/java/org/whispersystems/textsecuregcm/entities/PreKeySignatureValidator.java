/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.Base64;
import java.util.Collection;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

public abstract class PreKeySignatureValidator {
  public static final Counter INVALID_SIGNATURE_COUNTER =
      Metrics.counter(name(PreKeySignatureValidator.class, "invalidPreKeySignature"));

  public static final boolean validatePreKeySignatures(final String identityKeyB64, final Collection<SignedPreKey> spks) {
    try {
      final byte[] identityKeyBytes = Base64.getDecoder().decode(identityKeyB64);
      final ECPublicKey identityKey = Curve.decodePoint(identityKeyBytes, 0);

      final boolean success = spks.stream().allMatch(spk -> {
        final byte[] prekeyBytes = Base64.getDecoder().decode(spk.getPublicKey());
        final byte[] prekeySignatureBytes = Base64.getDecoder().decode(spk.getSignature());

        return identityKey.verifySignature(prekeyBytes, prekeySignatureBytes);
      });

      if (!success) {
        INVALID_SIGNATURE_COUNTER.increment();
      }

      return success;
    } catch (IllegalArgumentException | InvalidKeyException e) {
      INVALID_SIGNATURE_COUNTER.increment();
      return false;
    }
  }
}
