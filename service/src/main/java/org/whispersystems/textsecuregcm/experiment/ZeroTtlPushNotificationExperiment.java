/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import org.whispersystems.textsecuregcm.push.ZeroTtlNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.workers.IdleWakeupEligibilityChecker;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;

public class ZeroTtlPushNotificationExperiment extends IdleDevicePushNotificationExperiment {
  private static final LocalTime PREFERRED_NOTIFICATION_TIME = LocalTime.of(14, 0);

  private final ZeroTtlNotificationScheduler zeroTtlNotificationScheduler;

  public ZeroTtlPushNotificationExperiment(
      final IdleWakeupEligibilityChecker idleWakeupEligibilityChecker,
      final ZeroTtlNotificationScheduler zeroTtlNotificationScheduler) {
    super(idleWakeupEligibilityChecker);
    this.zeroTtlNotificationScheduler = zeroTtlNotificationScheduler;
  }

  @Override
  boolean isIdleDeviceEligible(final Account account, final Device idleDevice, final DeviceLastSeenState state) {
    return state.pushTokenType() == DeviceLastSeenState.PushTokenType.FCM;
  }

  @Override
  public String getExperimentName() {
    return "zero-ttl-notification";
  }

  @Override
  public Class<DeviceLastSeenState> getStateClass() {
    return DeviceLastSeenState.class;
  }

  @Override
  public CompletableFuture<Void> applyExperimentTreatment(final Account account, final Device device) {
    return zeroTtlNotificationScheduler.scheduleNotification(account, device, PREFERRED_NOTIFICATION_TIME, true);
  }

  @Override
  public CompletableFuture<Void> applyControlTreatment(final Account account, final Device device) {
    return zeroTtlNotificationScheduler.scheduleNotification(account, device, PREFERRED_NOTIFICATION_TIME, false);
  }
}
