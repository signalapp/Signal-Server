/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

/**
 * A captcha assessment
 *
 * @param valid whether the captcha was passed
 * @param score string representation of the risk level
 */
public record AssessmentResult(boolean valid, String score) {

  public static AssessmentResult invalid() {
    return new AssessmentResult(false, "");
  }

  /**
   * Map a captcha score in [0.0, 1.0] to a low cardinality discrete space in [0, 100] suitable for use in metrics
   */
  static String scoreString(final float score) {
    final int x = Math.round(score * 10); // [0, 10]
    return Integer.toString(x * 10); // [0, 100] in increments of 10
  }
}
