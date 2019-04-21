package org.whispersystems.dispatch.redis.protocol;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class StringReplyHeaderTest {

  @Test
  public void testNull() {
    try {
      new StringReplyHeader(null);
      throw new AssertionError();
    } catch (IOException e) {
      // good
    }
  }

  @Test
  public void testBadNumber() {
    try {
      new StringReplyHeader("$100A");
      throw new AssertionError();
    } catch (IOException e) {
      // good
    }
  }

  @Test
  public void testBadPrefix() {
    try {
      new StringReplyHeader("*");
      throw new AssertionError();
    } catch (IOException e) {
      // good
    }
  }

  @Test
  public void testValid() throws IOException {
    assertEquals(1000, new StringReplyHeader("$1000").getStringLength());
  }


}
