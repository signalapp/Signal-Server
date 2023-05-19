/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import java.util.Base64;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyType;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;

public final class KeysHelper {
  public static String serializeIdentityKey(ECKeyPair keyPair) {
    return Base64.getEncoder().encodeToString(keyPair.getPublicKey().serialize());
  }

  public static PreKey ecPreKey(final long id) {
    return new PreKey(id, Curve.generateKeyPair().getPublicKey().serialize());
  }
  
  public static SignedPreKey signedECPreKey(long id, final ECKeyPair identityKeyPair) {
    final byte[] pubKey = Curve.generateKeyPair().getPublicKey().serialize();
    final byte[] sig = identityKeyPair.getPrivateKey().calculateSignature(pubKey);
    return new SignedPreKey(id, pubKey, sig);
  }

  public static SignedPreKey signedKEMPreKey(long id, final ECKeyPair identityKeyPair) {
    final byte[] pubKey = KEMKeyPair.generate(KEMKeyType.KYBER_1024).getPublicKey().serialize();
    final byte[] sig = identityKeyPair.getPrivateKey().calculateSignature(pubKey);
    return new SignedPreKey(id, pubKey, sig);
  }
}
