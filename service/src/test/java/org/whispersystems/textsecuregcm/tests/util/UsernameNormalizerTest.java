package org.whispersystems.textsecuregcm.tests.util;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.UsernameNormalizer;

import static org.assertj.core.api.Assertions.assertThat;

public class UsernameNormalizerTest {

  @Test
  public void usernameNormalization() {
    assertThat(UsernameNormalizer.normalize("TeST")).isEqualTo("test");
    assertThat(UsernameNormalizer.normalize("TeST_")).isEqualTo("test_");
    assertThat(UsernameNormalizer.normalize("TeST_.123")).isEqualTo("test_.123");
  }

}
