package org.whispersystems.textsecuregcm.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UnidentifiedAccessChecksumTest {
  @ParameterizedTest
  @MethodSource
  public void generateFor(final byte[] unidentifiedAccessKey, final byte[] expectedChecksum) {
    final byte[] checksum = UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey);

    assertArrayEquals(expectedChecksum, checksum);
  }

  private static Stream<Arguments> generateFor() {
    return Stream.of(
        Arguments.of(Base64.getDecoder().decode("hqqo9upWeC0HSHOSJcXl/Q=="),
            Base64.getDecoder().decode("2DNxpQCjTefuEhdvJayIbAVUcZSXotu8nqXwWr+q6hI=")),
        Arguments.of(Base64.getDecoder().decode("0bNEmhGzmxBsDYhEhk+bAw=="),
            Base64.getDecoder().decode("gJTodQfP8TUITZhvrWr0t1siDZXYxRQ/qdpNB8jC+yc="))
    );
  }

  @Test
  public void generateForIllegalArgument() {
    final byte[] invalidLengthUnidentifiedAccessKey = new byte[15];
    new SecureRandom().nextBytes(invalidLengthUnidentifiedAccessKey);

    assertThrows(IllegalArgumentException.class, () -> UnidentifiedAccessChecksum.generateFor(invalidLengthUnidentifiedAccessKey));
  }
}
