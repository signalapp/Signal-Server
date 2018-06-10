package org.whispersystems.textsecuregcm.util;

import java.io.IOException;

public class Base64 {
  /**
   * Encodes a byte array into Base64 notation.
   * Does not GZip-compress data.
   *
   * @param source The data to convert
   * @return The data in Base64-encoded form
   */
  public static String encodeBytes(byte[] source) {
    return java.util.Base64.getEncoder().encodeToString(source);
  }   // end encodeBytes

  /**
   * Encodes a byte array into Base64 notation. But don't add padding.
   * Does not GZip-compress data.
   *
   * @param source The data to convert
   * @return The data in Base64-encoded form
   */
  public static String encodeBytesWithoutPadding(byte[] source) {
    return java.util.Base64.getEncoder().withoutPadding().encodeToString(source);
  }

  /**
   * Decodes data from Base64 notation
   *
   * @param s the string to decode
   * @return the decoded data
   * @throws java.io.IOException If there is a problem
   */
  public static byte[] decode(String s) throws java.io.IOException {
    try {
      return java.util.Base64.getDecoder().decode(s);
    } catch (Exception e) {
      throw  new IOException(e);
    }
  }


  /**
   * Decodes data from Base64 notation but do not add padding.
   *
   * @param source the string to decode
   * @return the decoded data
   * @throws java.io.IOException If there is a problem
   */
  public static byte[] decodeWithoutPadding(String source) throws java.io.IOException {
    int padding = source.length() % 4;

    if (padding == 1) source = source + "=";
    else if (padding == 2) source = source + "==";
    else if (padding == 3) source = source + "=";

    return decode(source);
  }


}   // end class Base64
