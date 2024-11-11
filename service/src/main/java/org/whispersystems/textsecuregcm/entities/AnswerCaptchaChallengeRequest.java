/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public class AnswerCaptchaChallengeRequest extends AnswerChallengeRequest {

  @Schema(description = "The value of the token field from the server's 428 response")
  @NotBlank
  private String token;

  @Schema(
      description = "A string representing a solved captcha",
      example = "signal-hcaptcha.30b01b46-d8c9-4c30-bbd7-9719acfe0c10.challenge.abcdefg1345")
  @NotBlank
  private String captcha;

  public String getToken() {
    return token;
  }

  public String getCaptcha() {
    return captcha;
  }
}
