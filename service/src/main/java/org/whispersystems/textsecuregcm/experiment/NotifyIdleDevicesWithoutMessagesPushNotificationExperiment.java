package org.whispersystems.textsecuregcm.experiment;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import reactor.core.publisher.Flux;
import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.Map;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;

public class NotifyIdleDevicesWithoutMessagesPushNotificationExperiment implements PushNotificationExperiment<DeviceLastSeenState> {

  private final MessagesManager messagesManager;
  private final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;

  private static final LocalTime PREFERRED_NOTIFICATION_TIME = LocalTime.of(14, 0);

  private static final Logger log = LoggerFactory.getLogger(NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.class);

  @VisibleForTesting
  enum Population {
    APNS_CONTROL,
    APNS_EXPERIMENT,
    FCM_CONTROL,
    FCM_EXPERIMENT
  }

  @VisibleForTesting
  enum Outcome {
    DELETED,
    UNINSTALLED,
    REACTIVATED,
    UNCHANGED
  }

  public NotifyIdleDevicesWithoutMessagesPushNotificationExperiment(final MessagesManager messagesManager,
      final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler) {

    this.messagesManager = messagesManager;
    this.idleDeviceNotificationScheduler = idleDeviceNotificationScheduler;
  }

  @Override
  public String getExperimentName() {
    return "notify-idle-devices-without-messages";
  }

  @Override
  public CompletableFuture<Boolean> isDeviceEligible(final Account account, final Device device) {

    if (!hasPushToken(device)) {
      return CompletableFuture.completedFuture(false);
    }

    if (!idleDeviceNotificationScheduler.isIdle(device)) {
      return CompletableFuture.completedFuture(false);
    }

    return messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device)
        .thenApply(mayHavePersistedMessages -> !mayHavePersistedMessages);
  }

  @VisibleForTesting
  static boolean hasPushToken(final Device device) {
    // Exclude VOIP tokens since they have their own, distinct delivery mechanism
    return !StringUtils.isAllBlank(device.getApnId(), device.getGcmId()) && StringUtils.isBlank(device.getVoipApnId());
  }

  @Override
  public DeviceLastSeenState getState(@Nullable final Account account, @Nullable final Device device) {
    if (account != null && device != null) {
      final DeviceLastSeenState.PushTokenType pushTokenType = StringUtils.isNotBlank(device.getApnId())
          ? DeviceLastSeenState.PushTokenType.APNS
          : DeviceLastSeenState.PushTokenType.FCM;

      return new DeviceLastSeenState(true, device.getCreated(), hasPushToken(device), device.getLastSeen(), pushTokenType);
    } else {
      return DeviceLastSeenState.MISSING_DEVICE_STATE;
    }
  }

  @Override
  public CompletableFuture<Void> applyExperimentTreatment(final Account account, final Device device) {
    return idleDeviceNotificationScheduler.scheduleNotification(account, device.getId(), PREFERRED_NOTIFICATION_TIME);
  }

  @Override
  public void analyzeResults(final Flux<PushNotificationExperimentSample<DeviceLastSeenState>> samples) {
    final Map<Population, Map<Outcome, Integer>> contingencyTable = new EnumMap<>(Population.class);

    for (final Population population : Population.values()) {
      final Map<Outcome, Integer> countsByOutcome = new EnumMap<>(Outcome.class);

      for (final Outcome outcome : Outcome.values()) {
        countsByOutcome.put(outcome, 0);
      }

      contingencyTable.put(population, countsByOutcome);
    }

    samples.doOnNext(sample -> contingencyTable.get(getPopulation(sample)).merge(getOutcome(sample), 1, Integer::sum))
        .then()
        .block();

    final StringBuilder reportBuilder = new StringBuilder("population,deleted,uninstalled,reactivated,unchanged\n");

    for (final Population population : Population.values()) {
      final Map<Outcome, Integer> countsByOutcome = contingencyTable.get(population);

      reportBuilder.append(population.name());
      reportBuilder.append(",");
      reportBuilder.append(countsByOutcome.getOrDefault(Outcome.DELETED, 0));
      reportBuilder.append(",");
      reportBuilder.append(countsByOutcome.getOrDefault(Outcome.UNINSTALLED, 0));
      reportBuilder.append(",");
      reportBuilder.append(countsByOutcome.getOrDefault(Outcome.REACTIVATED, 0));
      reportBuilder.append(",");
      reportBuilder.append(countsByOutcome.getOrDefault(Outcome.UNCHANGED, 0));
      reportBuilder.append("\n");
    }

    log.info(reportBuilder.toString());
  }

  @VisibleForTesting
  static Population getPopulation(final PushNotificationExperimentSample<DeviceLastSeenState> sample) {
    assert sample.initialState() != null && sample.initialState().pushTokenType() != null;

    return switch (sample.initialState().pushTokenType()) {
      case APNS -> sample.inExperimentGroup() ? Population.APNS_EXPERIMENT : Population.APNS_CONTROL;
      case FCM -> sample.inExperimentGroup() ? Population.FCM_EXPERIMENT : Population.FCM_CONTROL;
    };
  }

  @VisibleForTesting
  static Outcome getOutcome(final PushNotificationExperimentSample<DeviceLastSeenState> sample) {
    final Outcome outcome;

    if (!sample.finalState().deviceExists() || sample.initialState().createdAtMillis() != sample.finalState().createdAtMillis()) {
      outcome = Outcome.DELETED;
    } else if (!sample.finalState().hasPushToken()) {
      outcome = Outcome.UNINSTALLED;
    } else if (sample.initialState().lastSeenMillis() != sample.finalState().lastSeenMillis()) {
      outcome = Outcome.REACTIVATED;
    } else {
      outcome = Outcome.UNCHANGED;
    }

    return outcome;
  }
}
