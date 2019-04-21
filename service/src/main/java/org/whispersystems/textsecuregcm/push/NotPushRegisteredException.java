package org.whispersystems.textsecuregcm.push;

public class NotPushRegisteredException extends Exception {
  public NotPushRegisteredException(String s) {
    super(s);
  }

  public NotPushRegisteredException(Exception e) {
    super(e);
  }
}
