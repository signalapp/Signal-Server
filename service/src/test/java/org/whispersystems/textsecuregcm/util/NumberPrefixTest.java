/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

class NumberPrefixTest {

  @Test
  void testPrefixes() {
    assertThat(Util.getNumberPrefix("+14151234567")).isEqualTo("+14151");
    assertThat(Util.getNumberPrefix("+22587654321")).isEqualTo("+2258765");
    assertThat(Util.getNumberPrefix("+298654321")).isEqualTo("+2986543");
    assertThat(Util.getNumberPrefix("+12")).isEqualTo("+12");
  }

}
