package org.whispersystems.textsecuregcm.crypto;

public class ECKeyPair {

  private final ECPublicKey  publicKey;
  private final ECPrivateKey privateKey;

  ECKeyPair(ECPublicKey publicKey, ECPrivateKey privateKey) {
    this.publicKey = publicKey;
    this.privateKey = privateKey;
  }

  public ECPublicKey getPublicKey() {
    return publicKey;
  }

  public ECPrivateKey getPrivateKey() {
    return privateKey;
  }
}