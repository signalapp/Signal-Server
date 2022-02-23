/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.recaptcha;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import javax.annotation.Nonnull;

public class TransitionalRecaptchaClient implements RecaptchaClient {

  @VisibleForTesting
  static final String SEPARATOR = ".";
  @VisibleForTesting
  static final String V2_PREFIX = "signal-recaptcha-v2" + SEPARATOR;

  private final LegacyRecaptchaClient legacyRecaptchaClient;
  private final EnterpriseRecaptchaClient enterpriseRecaptchaClient;

  public TransitionalRecaptchaClient(
      @Nonnull final LegacyRecaptchaClient legacyRecaptchaClient,
      @Nonnull final EnterpriseRecaptchaClient enterpriseRecaptchaClient) {
    this.legacyRecaptchaClient = Objects.requireNonNull(legacyRecaptchaClient);
    this.enterpriseRecaptchaClient = Objects.requireNonNull(enterpriseRecaptchaClient);
  }

  @Override
  public boolean verify(@Nonnull final String token, @Nonnull final String ip) {
    if (token.startsWith(V2_PREFIX)) {
      final String[] actionAndToken = parseV2ActionAndToken(token.substring(V2_PREFIX.length()));
      return enterpriseRecaptchaClient.verify(actionAndToken[1], ip, actionAndToken[0]);
    } else {
      return legacyRecaptchaClient.verify(token, ip);
    }
  }

  /**
   * Parses the token and action (if any) from {@code input}. The expected input format is: {@code [action:]token}.
   * <p>
   * For action to be optional, there is a strong assumption that the token will never contain a {@value  SEPARATOR}.
   * Observation suggests {@code token} is base-64 encoded. In practice, an action should always be present, but we
   * don’t need to be strict.
   */
  static String[] parseV2ActionAndToken(final String input) {
    String[] actionAndToken = input.split("\\" + SEPARATOR, 2);

    if (actionAndToken.length == 1) {
      // there was no ":" delimiter; assume we only have a token
      return new String[]{null, actionAndToken[0]};
    }

    return actionAndToken;
  }
}
