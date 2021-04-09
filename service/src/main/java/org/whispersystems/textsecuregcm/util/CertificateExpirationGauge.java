/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.codahale.metrics.CachedGauge;
import org.bouncycastle.openssl.PEMReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Measures and reports the number of days until a certificate expires.
 */
public class CertificateExpirationGauge extends CachedGauge<Long> {

    private final Instant certificateExpiration;

    public CertificateExpirationGauge(final X509Certificate certificate) {
        super(1, TimeUnit.HOURS);

        certificateExpiration = certificate.getNotAfter().toInstant();
    }

    @Override
    protected Long loadValue() {
        return Duration.between(Instant.now(), certificateExpiration).toDays();
    }
}
