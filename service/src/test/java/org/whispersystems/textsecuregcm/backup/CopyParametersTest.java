/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CopyParametersTest {

  @ParameterizedTest
  @CsvSource({
      "0, 64",
      "15, 64",
      "16, 80",
      "2_147_483_647, 2_147_483_696"
  })
  void destinationObjectSize(final int inputSize, final long expectedOutputSize) {
    assertEquals(expectedOutputSize, CopyParameters.destinationObjectSize(inputSize));
  }

  @Test
  void destinationObjectSizeNegative() {
    assertThrows(IllegalArgumentException.class, () -> CopyParameters.destinationObjectSize(-1));
  }
}
