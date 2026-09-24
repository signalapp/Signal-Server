/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BoringSslServerKeyManagerTest {

  @ParameterizedTest
  @CsvSource(nullValues = "null", value = {
      "ed25519, EdDSA",
      "ecdsa_sha256, EC",
      "ecdsa_secp256r1_sha256, EC",
      "rsa_pss_rsae_sha256, RSA",
      "rsa_pss_rsae_sha512, RSA",
      "rsa_pkcs1_sha256, null",
      "rsa_pss_pss_sha256, null",
      "ed448, null",
  })
  void getJdkKeyType(final String boringSslSignatureAlgorithm, @Nullable final String expectedKeyType) {
    assertEquals(Optional.ofNullable(expectedKeyType), BoringSslServerKeyManager.getJdkKeyType(boringSslSignatureAlgorithm));
  }
}
