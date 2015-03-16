package org.whispersystems.dispatch.redis.protocol;

import java.io.IOException;

public class ArrayReplyHeader {

  private final int elementCount;

  public ArrayReplyHeader(String header) throws IOException {
    if (header == null || header.length() < 2 || header.charAt(0) != '*') {
      throw new IOException("Invalid array reply header: " + header);
    }

    try {
      this.elementCount = Integer.parseInt(header.substring(1));
    } catch (NumberFormatException e) {
      throw new IOException(e);
    }
  }

  public int getElementCount() {
    return elementCount;
  }
}
