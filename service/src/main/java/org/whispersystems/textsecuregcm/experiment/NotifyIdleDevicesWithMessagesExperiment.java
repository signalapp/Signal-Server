package org.whispersystems.textsecuregcm.experiment;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;

public class NotifyIdleDevicesWithMessagesExperiment extends IdleDevicePushNotificationExperiment {

  private final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;
  private final MessagesManager messagesManager;

  @VisibleForTesting
  static final Duration MIN_IDLE_DURATION = Duration.ofDays(3);

  @VisibleForTesting
  static final Duration MAX_IDLE_DURATION = Duration.ofDays(14);

  @VisibleForTesting
  static final LocalTime PREFERRED_NOTIFICATION_TIME = LocalTime.of(14, 0);

  public NotifyIdleDevicesWithMessagesExperiment(final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler,
      final MessagesManager messagesManager,
      final Clock clock) {

    super(clock);

    this.idleDeviceNotificationScheduler = idleDeviceNotificationScheduler;
    this.messagesManager = messagesManager;
  }

  @Override
  protected Duration getMinIdleDuration() {
    return MIN_IDLE_DURATION;
  }

  @Override
  protected Duration getMaxIdleDuration() {
    return MAX_IDLE_DURATION;
  }

  @Override
  public String getExperimentName() {
    return "notify-idle-devices-with-messages";
  }

  @Override
  public CompletableFuture<Boolean> isDeviceEligible(final Account account, final Device device) {

    if (!hasPushToken(device)) {
      return CompletableFuture.completedFuture(false);
    }

    if (!isIdle(device)) {
      return CompletableFuture.completedFuture(false);
    }

    return messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device);
  }

  @Override
  public Class<DeviceLastSeenState> getStateClass() {
    return DeviceLastSeenState.class;
  }

  @Override
  public CompletableFuture<Void> applyExperimentTreatment(final Account account, final Device device) {
    return idleDeviceNotificationScheduler.scheduleNotification(account, device, PREFERRED_NOTIFICATION_TIME);
  }
}
