/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import java.time.Duration;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public record PushNotification(PushToken<?> pushToken,
                               NotificationType notificationType,
                               @Nullable Object data,
                               @Nullable Account destination,
                               @Nullable Device destinationDevice,
                               boolean urgent,
                               @Nullable Duration ttl) {

  // APNs allows up to 30 days, but FCM allows a max of 28. See:
  //
  // - https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns
  // - https://firebase.google.com/docs/cloud-messaging/customize-messages/setting-message-lifespan
  private static final Duration MAX_TTL = Duration.ofDays(28);

  public enum NotificationType {
    NOTIFICATION,
    ATTEMPT_LOGIN_NOTIFICATION_HIGH_PRIORITY,
    CHALLENGE,
    RATE_LIMIT_CHALLENGE,
    VERIFICATION_CODE_REQUESTED
  }

  public enum TokenType {
    WEBPUSH,
    FCM,
    APN
  }

  public PushNotification {
    if (ttl != null && ttl.compareTo(MAX_TTL) > 0) {
      throw new IllegalArgumentException("TTL must not be longer than " + MAX_TTL);
    }
  }

  public sealed interface PushToken<T> permits PushToken.FCM, PushToken.APN, PushToken.WEBPUSH {
    T value();
    TokenType type();

    default boolean isBlank() {
      if (value() == null) return true;
      return switch(value()) {
        case String s -> StringUtils.isBlank(s);
        default -> false;
      };
    }

    public record FCM(String value) implements PushToken<String> {
      public TokenType type() { return TokenType.FCM; }
    }
    public record APN(String value) implements PushToken<String> {
      public TokenType type() { return TokenType.APN; }
    }
    public record WEBPUSH(WebPushSubscription value) implements PushToken<WebPushSubscription> {
      public TokenType type() { return TokenType.WEBPUSH; }
    }
  }

  public TokenType tokenType() {
    return pushToken().type();
  }
}
