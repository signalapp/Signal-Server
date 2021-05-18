/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

class AsnTableTest {

    @Test
    void getAsn() throws IOException {
      try (final InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("ip2asn-test.tsv"))) {
        final AsnTable asnTable = new AsnTable(reader);

        assertEquals(Optional.of(7922L), asnTable.getAsn((Inet4Address) Inet4Address.getByName("50.79.54.1")));
        assertEquals(Optional.of(7552L), asnTable.getAsn((Inet4Address) Inet4Address.getByName("27.79.32.1")));
        assertEquals(Optional.empty(), asnTable.getAsn((Inet4Address) Inet4Address.getByName("5.182.202.1")));
        assertEquals(Optional.empty(), asnTable.getAsn((Inet4Address) Inet4Address.getByName("32.79.117.1")));
        assertEquals(Optional.empty(), asnTable.getAsn((Inet4Address) Inet4Address.getByName("10.0.0.1")));
      }
    }

    @Test
    void getCountryCode() throws IOException {
      try (final InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("ip2asn-test.tsv"))) {
        final AsnTable asnTable = new AsnTable(reader);

        assertEquals(Optional.of("US"), asnTable.getCountryCode(7922));
        assertEquals(Optional.of("VN"), asnTable.getCountryCode(7552));
        assertEquals(Optional.empty(), asnTable.getCountryCode(1234));
      }
    }

    @Test
    void ipToLong() throws UnknownHostException {
      assertEquals(0x00000000ffffffffL, AsnTable.ipToLong((Inet4Address) Inet4Address.getByName("255.255.255.255")));
      assertEquals(0x0000000000000001L, AsnTable.ipToLong((Inet4Address) Inet4Address.getByName("0.0.0.1")));
    }
}
