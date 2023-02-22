/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.push.PushNotification;

public record UpdateVerificationSessionRequest(@Nullable String pushToken,
                                               @Nullable PushTokenType pushTokenType,
                                               @Nullable String pushChallenge,
                                               @Nullable String captcha,
                                               @Nullable String mcc,
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
