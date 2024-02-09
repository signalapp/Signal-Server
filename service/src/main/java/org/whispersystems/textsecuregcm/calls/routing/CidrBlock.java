/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Can be used to check if an IP is in the CIDR block
 */
public interface CidrBlock {

  boolean ipInBlock(InetAddress address);

  static CidrBlock parseCidrBlock(String cidrBlock, int defaultBlockSize) {
    String[] splits = cidrBlock.split("/");
    if(splits.length > 2) {
      throw new IllegalArgumentException("Invalid cidr block format, expected {address}/{blocksize}");
    }

    try {
      int blockSize = splits.length == 2 ? Integer.parseInt(splits[1]) : defaultBlockSize;
      return parseCidrBlockInner(splits[0], blockSize);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Invalid block size specified: '%s'", splits[1]));
    }
  }

  static CidrBlock parseCidrBlock(String cidrBlock) {
    String[] splits = cidrBlock.split("/");
    if (splits.length != 2) {
      throw new IllegalArgumentException("Invalid cidr block format, expected {address}/{blocksize}");
    }

    try {
      int blockSize = Integer.parseInt(splits[1]);
      return parseCidrBlockInner(splits[0], blockSize);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("Invalid block size specified: '%s'", splits[1]));
    }
  }

  private static CidrBlock parseCidrBlockInner(String rawAddress, int blockSize) {
    try {
      InetAddress address = InetAddress.getByName(rawAddress);
      if(address instanceof Inet4Address) {
        return IpV4CidrBlock.of((Inet4Address) address, blockSize);
      } else if (address instanceof Inet6Address) {
        return IpV6CidrBlock.of((Inet6Address) address, blockSize);
      } else {
        throw new IllegalArgumentException("Must be an ipv4 or ipv6 string");
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }

  record IpV4CidrBlock(int subnet, int subnetMask, int cidrBlockSize) implements CidrBlock {
    public static IpV4CidrBlock of(Inet4Address subnet, int cidrBlockSize) {
      if(cidrBlockSize > 32 || cidrBlockSize < 0) {
        throw new IllegalArgumentException("Invalid cidrBlockSize");
      }

      int subnetMask = mask(cidrBlockSize);
      int maskedIp = ipToInt(subnet) & subnetMask;
      return new IpV4CidrBlock(maskedIp, subnetMask, cidrBlockSize);
    }

    public boolean ipInBlock(InetAddress address) {
      if(!(address instanceof Inet4Address)) {
        return false;
      }
      int ip = ipToInt((Inet4Address) address);
      return (ip & subnetMask) == subnet;
    }

    private static int ipToInt(Inet4Address address) {
      byte[] octets = address.getAddress();
      return  (octets[0] & 0xff) << 24 |
          (octets[1] & 0xff) << 16 |
          (octets[2] & 0xff) << 8 |
          octets[3] & 0xff;
    }

    private static int mask(int cidrBlockSize) {
      return (int) (-1L << (32 - cidrBlockSize));
    }

    public static int maskToSize(Inet4Address address, int cidrBlockSize) {
      return ipToInt(address) & mask(cidrBlockSize);
    }
  }

  record IpV6CidrBlock(BigInteger subnet, BigInteger subnetMask, int cidrBlockSize) implements CidrBlock {

    private static final BigInteger MINUS_ONE = BigInteger.valueOf(-1);

    public static IpV6CidrBlock of(Inet6Address subnet, int cidrBlockSize) {
      if(cidrBlockSize > 128 || cidrBlockSize < 0) {
        throw new IllegalArgumentException("Invalid cidrBlockSize");
      }

      BigInteger subnetMask = mask(cidrBlockSize);
      BigInteger maskedIp = ipToInt(subnet).and(subnetMask);
      return new IpV6CidrBlock(maskedIp, subnetMask, cidrBlockSize);
    }

    public boolean ipInBlock(InetAddress address) {
      if(!(address instanceof Inet6Address)) {
        return false;
      }
      BigInteger ip = ipToInt((Inet6Address) address);
      return ip.and(subnetMask).equals(subnet);
    }

    private static BigInteger ipToInt(Inet6Address ipAddress) {
      byte[] octets = ipAddress.getAddress();
      assert octets.length == 16;

      return new BigInteger(octets);
    }

    private static BigInteger mask(int cidrBlockSize) {
      return MINUS_ONE.shiftLeft(128 - cidrBlockSize);
    }

    public static BigInteger maskToSize(Inet6Address address, int cidrBlockSize) {
      return ipToInt(address).and(mask(cidrBlockSize));
    }
  }
}
