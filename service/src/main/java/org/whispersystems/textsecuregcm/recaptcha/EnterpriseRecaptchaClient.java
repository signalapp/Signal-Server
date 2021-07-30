/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.recaptcha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnterpriseRecaptchaClient implements RecaptchaClient {
  private static final Logger logger = LoggerFactory.getLogger(EnterpriseRecaptchaClient.class);

  @Override
  public boolean verify(final String token, final String ip) {
    return false;
  }
}
