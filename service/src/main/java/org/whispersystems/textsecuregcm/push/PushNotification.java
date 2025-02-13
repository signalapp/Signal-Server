/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import javax.annotation.Nullable;
import java.util.Optional;

public record PushNotification(String deviceToken,
                               TokenType tokenType,
                               NotificationType notificationType,
                               @Nullable String data,
                               @Nullable Account destination,
                               @Nullable Device destinationDevice,
                               boolean urgent,
                               Optional<ExperimentalNotificationType> experimentalNotificationType) {

  public enum NotificationType {
    NOTIFICATION,
    ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY,
    CHALLENGE,
    RATE_LIMIT_CHALLENGE
  }

  public enum TokenType {
    FCM,
    APN
  }

  public enum ExperimentalNotificationType {
    ZERO_TTL,
    NON_COLLAPSIBLE
  }
}
