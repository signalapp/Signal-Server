package org.whispersystems.textsecuregcm.util;

import java.util.concurrent.ThreadLocalRandom;

public class TestRandomUtil {
  private TestRandomUtil() {}

  public static byte[] nextBytes(int numBytes) {
    final byte[] bytes = new byte[numBytes];
    ThreadLocalRandom.current().nextBytes(bytes);
    return bytes;
  }
}
