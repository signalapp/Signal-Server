package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class BackupMediaEncrypterTest {

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 15, 16, 17, 63, 64, 65, 1023, 1024, 1025})
  public void sizeCalc() {
    final MediaEncryptionParameters params = new MediaEncryptionParameters(
        TestRandomUtil.nextBytes(32),
        TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(16));
    final BackupMediaEncrypter encrypter = new BackupMediaEncrypter(params);
    assertThat(params.outputSize(1)).isEqualTo(encrypter.outputSize(1));
  }
}
