/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

public class CidrBlockTest {

  private HexFormat hex = HexFormat.ofDelimiter(":").withLowerCase();

  @Test
  public void testIPv4CidrBlockParseSuccess() {
    var actual = CidrBlock.parseCidrBlock("255.32.15.0/24");
    var expected = new CidrBlock.IpV4CidrBlock(0xFF_20_0F_00, 0xFFFFFF00, 24);

    assertThat(actual).isInstanceOf(CidrBlock.IpV4CidrBlock.class);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testIPv6CidrBlockParseSuccess() {
    var actual = CidrBlock.parseCidrBlock("2001:db8:b0aa::/48");
    var expected = new CidrBlock.IpV6CidrBlock(
        new BigInteger(hex.parseHex("20:01:0d:b8:b0:aa:00:00:00:00:00:00:00:00:00:00")),
        new BigInteger(hex.parseHex("FF:FF:FF:FF:FF:FF:00:00:00:00:00:00:00:00:00:00")),
        48
    );

    assertThat(actual).isInstanceOf(CidrBlock.IpV6CidrBlock.class);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testIPv4InBlock() throws UnknownHostException {
    var block = CidrBlock.parseCidrBlock("255.32.15.0/24");

    assertThat(block.ipInBlock(InetAddress.getByName("255.32.15.123"))).isTrue();
    assertThat(block.ipInBlock(InetAddress.getByName("255.32.15.0"))).isTrue();
    assertThat(block.ipInBlock(InetAddress.getByName("255.32.16.0"))).isFalse();
    assertThat(block.ipInBlock(InetAddress.getByName("255.33.15.0"))).isFalse();
    assertThat(block.ipInBlock(InetAddress.getByName("254.33.15.0"))).isFalse();
  }

  @Test
  public void testIPv6InBlock() throws UnknownHostException {
    var block = CidrBlock.parseCidrBlock("2001:db8:b0aa::/48");

    assertThat(block.ipInBlock(InetAddress.getByName("2001:db8:b0aa:1:1::"))).isTrue();
    assertThat(block.ipInBlock(InetAddress.getByName("2001:db8:b0aa:0:0::"))).isTrue();
    assertThat(block.ipInBlock(InetAddress.getByName("2001:db8:b0ab:1:1::"))).isFalse();
    assertThat(block.ipInBlock(InetAddress.getByName("2001:da8:b0aa:1:1::"))).isFalse();
  }
}
