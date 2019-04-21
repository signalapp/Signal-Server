package org.whispersystems.textsecuregcm.crypto;

public interface ECPrivateKey {
  public byte[] serialize();
  public int getType();
}

