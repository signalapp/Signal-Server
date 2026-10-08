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
      "Ed25519, EdDSA",
      "SHA256withECDSA, EC",
      "SHA512withECDSA, EC",
      "SHA256withRSA, RSA",
      "SHA512withRSA, RSA",
      "SHA256withRSA_PSS_PSS, null",
      "Ed448, null",
  })
  void getJdkKeyType(final String signatureAlgorithm, @Nullable final String expectedKeyType) {
    assertEquals(Optional.ofNullable(expectedKeyType), BoringSslServerKeyManager.getJdkKeyType(signatureAlgorithm));
  }
}
