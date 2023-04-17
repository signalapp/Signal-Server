/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import javax.annotation.Nullable;

public record PushNotification(String deviceToken,
                               TokenType tokenType,
                               NotificationType notificationType,
                               @Nullable String data,
                               @Nullable Account destination,
                               @Nullable Device destinationDevice,
                               boolean urgent) {

  public enum NotificationType {
    NOTIFICATION,
    ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY,
    @Deprecated ATTEMPT_LOGIN_NOTIFICATION_LOW_PRIORITY, // Temporary support for iOS clients; can be removed after 2023-06-12
    CHALLENGE,
    RATE_LIMIT_CHALLENGE
  }

  public enum TokenType {
    FCM,
    APN,
    APN_VOIP,
  }
}
