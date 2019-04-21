package org.whispersystems.textsecuregcm.entities;

public class CryptoEncodingException extends Exception {

  public CryptoEncodingException(String s) {
    super(s);
  }

  public CryptoEncodingException(Exception e) {
    super(e);
  }

}
