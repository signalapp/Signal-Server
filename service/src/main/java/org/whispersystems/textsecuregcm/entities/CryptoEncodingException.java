/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

public class CryptoEncodingException extends Exception {

  public CryptoEncodingException(String s) {
    super(s);
  }

  public CryptoEncodingException(Exception e) {
    super(e);
  }

}
