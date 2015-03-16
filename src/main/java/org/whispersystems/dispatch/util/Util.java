package org.whispersystems.dispatch.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Util {

  public static byte[] combine(byte[]... elements) {
    try {
      int sum = 0;

      for (byte[] element : elements) {
        sum += element.length;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(sum);

      for (byte[] element : elements) {
        baos.write(element);
      }

      return baos.toByteArray();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }


  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }
}
