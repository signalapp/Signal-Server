package org.whispersystems.textsecuregcm.util;

import java.util.Locale;

public final class UsernameNormalizer {
  private UsernameNormalizer() {}
  public static String normalize(final String username) {
    return username.toLowerCase(Locale.ROOT);
  }
}
