/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.asn;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.whispersystems.textsecuregcm.asn.AsnInfoProviderImpl.ip4BytesToLong;
import static org.whispersystems.textsecuregcm.asn.AsnInfoProviderImpl.ip6BytesToBigInteger;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class AsnInfoProviderImplTest {

  private static final String RESOURCE_NAME = "ip2asn-test.tsv";

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  void testAsnInfo() throws IOException {
    try (final InputStream tsvInputStream = getClass().getResourceAsStream(RESOURCE_NAME)) {
      final AsnInfoProvider asnInfoProvider = AsnInfoProviderImpl.fromTsv(requireNonNull(tsvInputStream));
      assertEquals(16625L, asnInfoProvider.lookup("2.16.112.0").get().asn());
      assertEquals(16625L, asnInfoProvider.lookup("2.16.112.255").get().asn());
      assertEquals(16625L, asnInfoProvider.lookup("2.16.113.0").get().asn());
      assertEquals(16625L, asnInfoProvider.lookup("2.16.113.123").get().asn());
      assertEquals(16625L, asnInfoProvider.lookup("2.16.113.255").get().asn());

      assertEquals("US", asnInfoProvider.lookup("2.16.113.255").get().regionCode());

      assertEquals(4690L, asnInfoProvider.lookup("2001:200:e00::").get().asn());
      assertEquals(4690L, asnInfoProvider.lookup("2001:200:ef0::").get().asn());
      assertEquals(4690L, asnInfoProvider.lookup("2001:200:eff:ffff::").get().asn());
      assertEquals(4690L, asnInfoProvider.lookup("2001:200:eff:ffff:ffff:ffff:ffff:ffff").get().asn());

      assertEquals("JP", asnInfoProvider.lookup("2001:200:eff:ffff:ffff:ffff:ffff:ffff").get().regionCode());

      assertTrue(asnInfoProvider.lookup("1.3.0.0").isEmpty());
      assertTrue(asnInfoProvider.lookup("1.4.127.255").isEmpty());
      assertTrue(asnInfoProvider.lookup("2001:4:113::").isEmpty());
      assertTrue(asnInfoProvider.lookup("0.0.0.0").isEmpty());
      assertTrue(asnInfoProvider.lookup("127.0.0.1").isEmpty());
      assertTrue(asnInfoProvider.lookup("not an ip").isEmpty());
    }
  }

  @Test
  void testBytesToLong() throws Exception {
    assertEquals(0x00000000ffffffffL, ip4BytesToLong((Inet4Address) InetAddress.getByName("255.255.255.255")));
    assertEquals(0x0000000000000001L, ip4BytesToLong((Inet4Address) InetAddress.getByName("0.0.0.1")));
    assertEquals(0x00000000ff00ff01L, ip4BytesToLong((Inet4Address) InetAddress.getByName("255.0.255.1")));

    final BigInteger start = ip6BytesToBigInteger((Inet6Address) InetAddress.getByName("2c0f:fff1:0:0:0:0:0:0"));
    final BigInteger end = ip6BytesToBigInteger((Inet6Address) InetAddress.getByName("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
    assertTrue(start.compareTo(BigInteger.ZERO) >= 0);
    assertTrue(end.compareTo(BigInteger.ZERO) >= 0);
    assertTrue(start.compareTo(end) <= 0);
  }
}
