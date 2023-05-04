/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import java.util.Base64;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;

public final class KeysHelper {
  public static String serializeIdentityKey(ECKeyPair keyPair) {
    return Base64.getEncoder().encodeToString(keyPair.getPublicKey().serialize());
  }
  
  public static SignedPreKey signedPreKey(long id, final ECKeyPair signingKey) {
    final byte[] pubKey = Curve.generateKeyPair().getPublicKey().serialize();
    final byte[] sig = signingKey.getPrivateKey().calculateSignature(pubKey);
    return new SignedPreKey(id, Base64.getEncoder().encodeToString(pubKey), Base64.getEncoder().encodeToString(sig));
  }
}
