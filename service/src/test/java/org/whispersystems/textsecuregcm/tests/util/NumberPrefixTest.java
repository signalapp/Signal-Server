package org.whispersystems.textsecuregcm.tests.util;

import org.junit.Test;
import org.whispersystems.textsecuregcm.util.Util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class NumberPrefixTest {

  @Test
  public void testPrefixes() {
    assertThat(Util.getNumberPrefix("+14151234567")).isEqualTo("+14151");
    assertThat(Util.getNumberPrefix("+22587654321")).isEqualTo("+2258765");
    assertThat(Util.getNumberPrefix("+298654321")).isEqualTo("+2986543");
    assertThat(Util.getNumberPrefix("+12")).isEqualTo("+12");
  }

}
