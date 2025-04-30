/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

public enum HandshakePattern {
  NK("Noise_NK_25519_ChaChaPoly_BLAKE2b"),
  IK("Noise_IK_25519_ChaChaPoly_BLAKE2b");

  private final String protocol;

  public String protocol() {
    return protocol;
  }


  HandshakePattern(String protocol) {
    this.protocol = protocol;
  }
}
