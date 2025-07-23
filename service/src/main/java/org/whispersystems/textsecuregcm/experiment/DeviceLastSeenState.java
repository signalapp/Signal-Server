package org.whispersystems.textsecuregcm.experiment;

import javax.annotation.Nullable;

public record DeviceLastSeenState(boolean deviceExists,
                                  // Registration IDs are not guaranteed to be unique across devices and re-registrations.
                                  // However, for this use case, we accept the possibility of collisions in order to
                                  // avoid storing plaintext device creation timestamps on the server.
                                  // This tradeoff is intentional and aligned with our privacy goals.
                                  int registrationId,
                                  boolean hasPushToken,
                                  long lastSeenMillis,
                                  @Nullable PushTokenType pushTokenType) {

  public static DeviceLastSeenState MISSING_DEVICE_STATE = new DeviceLastSeenState(false, 0, false, 0, null);

  public enum PushTokenType {
    APNS,
    FCM
  }
}
