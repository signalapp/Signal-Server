/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.io.IOException;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos.SenderCertificate;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ServerCertificate;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class CertificateGeneratorTest {

  private static final byte[] SIGNING_CERTIFICATE_DATA;
  // This arbitrary test ID is embedded in the serialized certificate
  private static final int SIGNING_CERTIFICATE_ID = 12;
  private static final ECPrivateKey SIGNING_KEY;
  private static final IdentityKey IDENTITY_KEY;
  private static final UUID ACI = UUID.randomUUID();
  private static final String E164 = PhoneNumberUtil.getInstance()
      .format(PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  static {
    try {
      SIGNING_CERTIFICATE_DATA = Base64.getDecoder().decode("CiUIDBIhBbTz4h1My+tt+vw+TVscgUe/DeHS0W02tPWAWbTO2xc3EkD+go4bJnU0AcnFfbOLKoiBfCzouZtDYMOVi69rE7r4U9cXREEqOkUmU2WJBjykAxWPCcSTmVTYHDw7hkSp/puG");
      SIGNING_KEY = new ECPrivateKey(Base64.getDecoder().decode("ABOxG29xrfq4E7IrW11Eg7+HBbtba9iiS0500YoBjn4="));
      IDENTITY_KEY = new IdentityKey(Base64.getDecoder().decode("BcxxDU9FGMda70E7+Uvm7pnQcEdXQ64aJCpPUeRSfcFo"));
    } catch (org.signal.libsignal.protocol.InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }

  @CartesianTest
  @ValueSource(booleans = {true, false})
  void testCreateFor(@CartesianTest.Values(booleans = {true, false}) boolean includeE164,
      @CartesianTest.Values(booleans = {true, false}) boolean embedSigner)
      throws IOException, org.signal.libsignal.protocol.InvalidKeyException {
    final Account account = mock(Account.class);
    final byte deviceId = 4;
    final CertificateGenerator certificateGenerator = new CertificateGenerator(
        SIGNING_CERTIFICATE_DATA, SIGNING_KEY, 1, embedSigner);

    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(IDENTITY_KEY);
    when(account.getUuid()).thenReturn(ACI);
    when(account.getNumber()).thenReturn(E164);

    final byte[] contents = certificateGenerator.createFor(account, deviceId, includeE164);
    final SenderCertificate fullCertificate = SenderCertificate.parseFrom(contents);
    final SenderCertificate.Certificate certificate = SenderCertificate.Certificate.parseFrom(fullCertificate.getCertificate());
    assertEquals(deviceId, certificate.getSenderDevice());
    assertEquals(UUIDUtil.toByteString(ACI), certificate.getSenderUuid());
    assertEquals(includeE164 ? E164 : "", certificate.getSenderE164());
    assertArrayEquals(IDENTITY_KEY.serialize(), certificate.getIdentityKey().toByteArray());
    assertTrue(certificate.getExpires() > 0);

    final ECPublicKey signingKey;
    if (embedSigner) {
      // Make sure we can produce certificates with embedded signers, in case of a future rotation
      assertFalse(certificate.hasSignerId());
      assertArrayEquals(SIGNING_CERTIFICATE_DATA, certificate.getSignerCertificate().toByteArray());

      final byte[] signingKeyBytes = ServerCertificate.Certificate.parseFrom(
          certificate.getSignerCertificate().getCertificate()).getKey().toByteArray();
      signingKey = new ECPublicKey(signingKeyBytes);
    } else {
      assertFalse(certificate.hasSignerCertificate());
      assertEquals(SIGNING_CERTIFICATE_ID, certificate.getSignerId());
      signingKey = SIGNING_KEY.publicKey();
    }

    assertTrue(signingKey
        .verifySignature(fullCertificate.getCertificate().toByteArray(), fullCertificate.getSignature().toByteArray()));
  }
}
