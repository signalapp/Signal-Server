package org.whispersystems.textsecuregcm.backup;

import java.io.IOException;

public class InvalidLengthException extends IOException {

  public InvalidLengthException(String s) {
    super(s);
  }
}
