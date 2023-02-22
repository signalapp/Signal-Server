/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.registration.MessageTransport;

public record VerificationCodeRequest(@NotNull Transport transport, @NotNull String client) {

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
