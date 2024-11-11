/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public class AnswerPushChallengeRequest extends AnswerChallengeRequest {

  @Schema(description = "A token provided to the client via a push payload")
  @NotBlank
  private String challenge;

  public String getChallenge() {
    return challenge;
  }
}
