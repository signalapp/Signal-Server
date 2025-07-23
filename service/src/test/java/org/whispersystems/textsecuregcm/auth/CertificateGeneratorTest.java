/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;

class CertificateGeneratorTest {

  private static final String SIGNING_CERTIFICATE = "CiUIDBIhBbTz4h1My+tt+vw+TVscgUe/DeHS0W02tPWAWbTO2xc3EkD+go4bJnU0AcnFfbOLKoiBfCzouZtDYMOVi69rE7r4U9cXREEqOkUmU2WJBjykAxWPCcSTmVTYHDw7hkSp/puG";
  private static final String SIGNING_KEY = "ABOxG29xrfq4E7IrW11Eg7+HBbtba9iiS0500YoBjn4=";
  private static final IdentityKey IDENTITY_KEY;

  static {
    try {
      IDENTITY_KEY = new IdentityKey(Base64.getDecoder().decode("BcxxDU9FGMda70E7+Uvm7pnQcEdXQ64aJCpPUeRSfcFo"));
    } catch (org.signal.libsignal.protocol.InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testCreateFor() throws IOException, InvalidKeyException, org.signal.libsignal.protocol.InvalidKeyException {
    final Account account = mock(Account.class);
    final byte deviceId = 4;
    final CertificateGenerator certificateGenerator = new CertificateGenerator(
        Base64.getDecoder().decode(SIGNING_CERTIFICATE),
        new ECPrivateKey(Base64.getDecoder().decode(SIGNING_KEY)), 1);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(IDENTITY_KEY);
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn("+18005551234");

    assertTrue(certificateGenerator.createFor(account, deviceId, true).length > 0);
    assertTrue(certificateGenerator.createFor(account, deviceId, false).length > 0);
  }
}
