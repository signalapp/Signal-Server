/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.Test;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CertificateExpirationGaugeTest {

    @Test
    public void loadValue() {
        final X509Certificate            certificate         = mock(X509Certificate.class);

        final long                       daysUntilExpiration = 17;

        final Instant                    now                 = Instant.now();
        final Instant                    later               = now.plus(Duration.ofDays(daysUntilExpiration)).plus(Duration.ofMinutes(1));

        when(certificate.getNotAfter()).thenReturn(new Date(later.toEpochMilli()));

        final CertificateExpirationGauge gauge               = new CertificateExpirationGauge(certificate);

        assertEquals(daysUntilExpiration, (long) gauge.loadValue());
    }
}
