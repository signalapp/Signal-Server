/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.junit.Test;
import org.whispersystems.textsecuregcm.crypto.Curve;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Base64;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CertificateGeneratorTest {

    private static final String SIGNING_CERTIFICATE = "CiUIDBIhBbTz4h1My+tt+vw+TVscgUe/DeHS0W02tPWAWbTO2xc3EkD+go4bJnU0AcnFfbOLKoiBfCzouZtDYMOVi69rE7r4U9cXREEqOkUmU2WJBjykAxWPCcSTmVTYHDw7hkSp/puG";
    private static final String SIGNING_KEY         = "ABOxG29xrfq4E7IrW11Eg7+HBbtba9iiS0500YoBjn4=";
    private static final String IDENTITY_KEY        = "BcxxDU9FGMda70E7+Uvm7pnQcEdXQ64aJCpPUeRSfcFo";

    @Test
    public void testCreateFor() throws IOException, InvalidKeyException {
        final Account account = mock(Account.class);
        final Device device = mock(Device.class);
        final CertificateGenerator certificateGenerator = new CertificateGenerator(Base64.decode(SIGNING_CERTIFICATE), Curve.decodePrivatePoint(Base64.decode(SIGNING_KEY)), 1);

        when(account.getIdentityKey()).thenReturn(IDENTITY_KEY);
        when(account.getUuid()).thenReturn(UUID.randomUUID());
        when(account.getNumber()).thenReturn("+18005551234");
        when(device.getId()).thenReturn(4L);

        assertTrue(certificateGenerator.createFor(account, device, true).length > 0);
        assertTrue(certificateGenerator.createFor(account, device, false).length > 0);
    }
}
