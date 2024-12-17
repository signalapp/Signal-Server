/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.push.PushNotification;

public record UpdateVerificationSessionRequest(
    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "The APNs or FCM device token to which a push challenge can be sent")
    @Nullable String pushToken,
    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "The type of push token")
    @Nullable PushTokenType pushTokenType,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Value received by the device in the push challenge")
    @Nullable String pushChallenge,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Captcha token returned after solving a captcha challenge")
    @Nullable String captcha,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Mobile country code of the phone subscriber")
    @Nullable String mcc,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = "Mobile network code of the phone subscriber")
    @Nullable String mnc) {

  public enum PushTokenType {
    @JsonProperty("apn")
    APN,
    @JsonProperty("fcm")
    FCM;

    public PushNotification.TokenType toTokenType() {
      return switch (this) {

        case APN -> PushNotification.TokenType.APN;
        case FCM -> PushNotification.TokenType.FCM;
      };
    }
  }

}
