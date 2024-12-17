/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.registration.MessageTransport;

public record VerificationCodeRequest(@Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Transport via which to send the verification code")
                                      @NotNull Transport transport,

                                      @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Client type to facilitate platform-specific SMS verification")
                                      @NotNull String client) {

  public enum Transport {
    @JsonProperty("sms")
    SMS,
    @JsonProperty("voice")
    VOICE;

    public MessageTransport toMessageTransport() {
      return switch (this) {
        case SMS -> MessageTransport.SMS;
        case VOICE -> MessageTransport.VOICE;
      };
    }
  }

}
