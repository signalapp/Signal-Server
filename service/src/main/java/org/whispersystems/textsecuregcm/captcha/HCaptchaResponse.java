/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Verify response returned by hcaptcha
 * <p>
 * see <a href="https://docs.hcaptcha.com/#verify-the-user-response-server-side">...</a>
 */
public class HCaptchaResponse {

  @JsonProperty
  boolean success;

  @JsonProperty(value = "challenge_ts")
  Instant challengeTs;

  @JsonProperty
  String hostname;

  @JsonProperty(value = "error-codes")
  List<String> errorCodes = Collections.emptyList();

  @JsonProperty
  float score;

  @JsonProperty(value = "score_reason")
  List<String> scoreReasons = Collections.emptyList();

  public HCaptchaResponse() {
  }

  @Override
  public String toString() {
    return "HCaptchaResponse{" +
        "success=" + success +
        ", challengeTs=" + challengeTs +
        ", hostname='" + hostname + '\'' +
        ", errorCodes=" + errorCodes +
        ", score=" + score +
        ", scoreReasons=" + scoreReasons +
        '}';
  }
}
