package org.whispersystems.dispatch.redis.protocol;


import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ArrayReplyHeaderTest {


  @Test(expected = IOException.class)
  public void testNull() throws IOException {
    new ArrayReplyHeader(null);
  }

  @Test(expected = IOException.class)
  public void testBadPrefix() throws IOException {
    new ArrayReplyHeader(":3");
  }

  @Test(expected = IOException.class)
  public void testEmpty() throws IOException {
    new ArrayReplyHeader("");
  }

  @Test(expected = IOException.class)
  public void testTruncated() throws IOException {
    new ArrayReplyHeader("*");
  }

  @Test(expected = IOException.class)
  public void testBadNumber() throws IOException {
    new ArrayReplyHeader("*ABC");
  }

  @Test
  public void testValid() throws IOException {
    assertEquals(4, new ArrayReplyHeader("*4").getElementCount());
  }









}
