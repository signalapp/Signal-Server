package org.whispersystems.textsecuregcm.experiment;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.workers.IdleWakeupEligibilityChecker;
import reactor.core.publisher.Flux;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

abstract class IdleDevicePushNotificationExperiment implements PushNotificationExperiment<DeviceLastSeenState> {

  private final IdleWakeupEligibilityChecker idleWakeupEligibilityChecker;

  private final Logger log = LoggerFactory.getLogger(getClass());

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

  protected IdleDevicePushNotificationExperiment(final IdleWakeupEligibilityChecker idleWakeupEligibilityChecker) {
    this.idleWakeupEligibilityChecker = idleWakeupEligibilityChecker;
  }

  @VisibleForTesting
  boolean hasPushToken(final Device device) {
    return !StringUtils.isAllBlank(device.getApnId(), device.getGcmId());
  }

  abstract boolean isIdleDeviceEligible(final Account account, final Device idleDevice, final DeviceLastSeenState state);

  @Override
  public CompletableFuture<Boolean> isDeviceEligible(final Account account, final Device device) {
    return idleWakeupEligibilityChecker.isDeviceEligible(account, device).thenApply(idle ->
        idle && isIdleDeviceEligible(account, device, getState(account, device)));
  }

  @Override
  public DeviceLastSeenState getState(@Nullable final Account account, @Nullable final Device device) {
    if (account != null && device != null) {
      final DeviceLastSeenState.PushTokenType pushTokenType;
      if (StringUtils.isNotBlank(device.getApnId())) {
        pushTokenType = DeviceLastSeenState.PushTokenType.APNS;
      } else if (StringUtils.isNotBlank(device.getGcmId())) {
        pushTokenType = DeviceLastSeenState.PushTokenType.FCM;
      } else {
        pushTokenType = null;
      }
      return new DeviceLastSeenState(true, device.getRegistrationId(IdentityType.ACI), hasPushToken(device), device.getLastSeen(), pushTokenType);
    } else {
      return DeviceLastSeenState.MISSING_DEVICE_STATE;
    }
  }

  @Override
  public void analyzeResults(final Flux<PushNotificationExperimentSample<DeviceLastSeenState>> samples) {
    final Map<Population, Map<Outcome, Integer>> contingencyTable = new EnumMap<>(Population.class);

    samples.doOnNext(sample ->
            contingencyTable.computeIfAbsent(getPopulation(sample), ignored -> new EnumMap<>(Outcome.class))
                .merge(getOutcome(sample), 1, Integer::sum))
        .then()
        .block();

    final StringBuilder reportBuilder = new StringBuilder("population,deleted,uninstalled,reactivated,unchanged\n");

    for (final Population population : Population.values()) {
      final Map<Outcome, Integer> countsByOutcome = contingencyTable.getOrDefault(population, Collections.emptyMap());

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

    assert sample.finalState() != null;

    if (!sample.finalState().deviceExists() || sample.initialState().registrationId() != sample.finalState().registrationId()) {
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
