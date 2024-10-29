/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class RegistrationCaptchaManager {

  private final CaptchaChecker captchaChecker;

  public RegistrationCaptchaManager(final CaptchaChecker captchaChecker) {
    this.captchaChecker = captchaChecker;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public Optional<AssessmentResult> assessCaptcha(final Optional<UUID> aci, final Optional<String> captcha, final String sourceHost, final String userAgent)
      throws IOException {
    return captcha.isPresent()
        ? Optional.of(captchaChecker.verify(aci, Action.REGISTRATION, captcha.get(), sourceHost, userAgent))
        : Optional.empty();
  }
}
