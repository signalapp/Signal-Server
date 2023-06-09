/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.protocol.kem.KEMKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyType;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.whispersystems.textsecuregcm.entities.ECPreKey;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;

public final class KeysHelper {

  public static ECPreKey ecPreKey(final long id) {
    return new ECPreKey(id, Curve.generateKeyPair().getPublicKey());
  }

  public static ECSignedPreKey signedECPreKey(long id, final ECKeyPair identityKeyPair) {
    final ECPublicKey pubKey = Curve.generateKeyPair().getPublicKey();
    final byte[] sig = identityKeyPair.getPrivateKey().calculateSignature(pubKey.serialize());
    return new ECSignedPreKey(id, pubKey, sig);
  }

  public static KEMSignedPreKey signedKEMPreKey(long id, final ECKeyPair identityKeyPair) {
    final KEMPublicKey pubKey = KEMKeyPair.generate(KEMKeyType.KYBER_1024).getPublicKey();
    final byte[] sig = identityKeyPair.getPrivateKey().calculateSignature(pubKey.serialize());
    return new KEMSignedPreKey(id, pubKey, sig);
  }
}
