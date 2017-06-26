package org.whispersystems.textsecuregcm.util;

import org.junit.Test;

import java.io.IOError;
import java.io.IOException;

import static org.junit.Assert.*;


public class Base64Test {
  @Test(expected = IOException.class)
  public void decodeThrowsIOErrorOnNonsense() throws Exception {
    Base64.decode("1");
  }

  @Test(expected = IOException.class)
  public void decodeWithoutPaddingThrowsIOErrorOnNonse() throws Exception {
    Base64.decodeWithoutPadding("1");
  }

}