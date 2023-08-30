package org.whispersystems.textsecuregcm.tests.util;

import java.util.Base64;
import java.util.Random;

public class ProfileTestHelper {
  public static String generateRandomBase64FromByteArray(final int byteArrayLength) {
    return encodeToBase64(generateRandomByteArray(byteArrayLength));
  }

  public static byte[] generateRandomByteArray(final int length) {
    byte[] byteArray = new byte[length];
    new Random().nextBytes(byteArray);
    return byteArray;
  }

  public static String encodeToBase64(final byte[] input) {
    return Base64.getEncoder().encodeToString(input);
  }
}
