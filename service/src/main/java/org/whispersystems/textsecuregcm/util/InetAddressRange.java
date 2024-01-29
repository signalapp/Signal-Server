/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * An InetAddressRange represents a contiguous range of IPv4 or IPv6 addresses.
 */
public class InetAddressRange {

  private final InetAddress networkAddress;

  private final byte[] networkAddressBytes;
  private final byte[] prefixMask;

  public InetAddressRange(final String cidrBlock) {
    final String[] components = cidrBlock.split("/");

    if (components.length != 2) {
      throw new IllegalArgumentException("Unexpected CIDR block notation: " + cidrBlock);
    }

    final int prefixLength;

    try {
      networkAddress = InetAddresses.forString(components[0]);
      prefixLength = Integer.parseInt(components[1]);

      if (prefixLength > networkAddress.getAddress().length * 8) {
        throw new IllegalArgumentException("Prefix length cannot exceed length of address");
      }
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Bad prefix length: " + components[1]);
    }

    networkAddressBytes = networkAddress.getAddress();
    prefixMask = generatePrefixMask(networkAddressBytes.length, prefixLength);
  }

  @VisibleForTesting
  static byte[] generatePrefixMask(final int addressLengthBytes, final int prefixLengthBits) {
    final byte[] prefixMask = new byte[addressLengthBytes];

    for (int i = 0; i < addressLengthBytes; i++) {
      final int bitsAvailable = Math.min(8, Math.max(0, prefixLengthBits - (i * 8)));
      prefixMask[i] = (byte) (0xff << (8 - bitsAvailable));
    }

    return prefixMask;
  }

  public boolean contains(final String name) {
    // InetAddresses.forString() throws "IllegalArgumentException" for anything that is not an IP address
    return contains(InetAddresses.forString(name));
  }

  public boolean contains(final InetAddress inetAddress) {
    if (!networkAddress.getClass().equals(inetAddress.getClass())) {
      return false;
    }

    final byte[] addressBytes = inetAddress.getAddress();

    for (int i = 0; i < addressBytes.length; i++) {
      if (((addressBytes[i] ^ networkAddressBytes[i]) & prefixMask[i]) != 0) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final InetAddressRange that = (InetAddressRange) o;

    if (!networkAddress.equals(that.networkAddress)) {
      return false;
    }
    return Arrays.equals(prefixMask, that.prefixMask);
  }

  @Override
  public int hashCode() {
    int result = networkAddress.hashCode();
    result = 31 * result + Arrays.hashCode(prefixMask);
    return result;
  }
}
