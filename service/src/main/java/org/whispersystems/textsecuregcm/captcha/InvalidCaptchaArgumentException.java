/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.captcha;

/**
 * Indicates that a captcha solution was malformed
 */
public class InvalidCaptchaArgumentException extends Exception {

  public InvalidCaptchaArgumentException(String message) {
      super(message);
  }
}
