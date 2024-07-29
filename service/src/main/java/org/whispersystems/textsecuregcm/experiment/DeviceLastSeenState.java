package org.whispersystems.textsecuregcm.experiment;

import javax.annotation.Nullable;

public record DeviceLastSeenState(boolean deviceExists,
                                  long createdAtMillis,
                                  boolean hasPushToken,
                                  long lastSeenMillis,
                                  @Nullable PushTokenType pushTokenType) {

  public static DeviceLastSeenState MISSING_DEVICE_STATE = new DeviceLastSeenState(false, 0, false, 0, null);

  public enum PushTokenType {
    APNS,
    FCM
  }
}
