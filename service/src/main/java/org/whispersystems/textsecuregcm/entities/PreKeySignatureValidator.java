/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import static com.codahale.metrics.MetricRegistry.name;
import io.micrometer.core.instrument.Metrics;
import java.util.Base64;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

public abstract class PreKeySignatureValidator {
  public static final boolean validatePreKeySignature(final String identityKeyB64, final SignedPreKey spk) {
    try {
      final byte[] identityKeyBytes = Base64.getDecoder().decode(identityKeyB64);
      final byte[] prekeyBytes = Base64.getDecoder().decode(spk.getPublicKey());
      final byte[] prekeySignatureBytes = Base64.getDecoder().decode(spk.getSignature());

      final ECPublicKey identityKey = Curve.decodePoint(identityKeyBytes, 0);

      return identityKey.verifySignature(prekeyBytes, prekeySignatureBytes);
    } catch (IllegalArgumentException | InvalidKeyException e) {
      Metrics.counter(name(PreKeySignatureValidator.class, "invalidPreKeySignature")).increment();
      return false;
    }
  }
}
