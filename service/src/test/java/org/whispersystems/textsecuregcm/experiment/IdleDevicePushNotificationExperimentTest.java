package org.whispersystems.textsecuregcm.experiment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class IdleDevicePushNotificationExperimentTest {

  protected static final Instant CURRENT_TIME = Instant.now();
  
  protected abstract IdleDevicePushNotificationExperiment getExperiment();

  @ParameterizedTest
  @MethodSource
  void hasPushToken(final Device device, final boolean expectHasPushToken) {
    assertEquals(expectHasPushToken, getExperiment().hasPushToken(device));
  }

  private static List<Arguments> hasPushToken() {
    final List<Arguments> arguments = new ArrayList<>();

    {
      // No token at all
      final Device device = mock(Device.class);

      arguments.add(Arguments.of(device, false));
    }

    {
      // FCM token
      final Device device = mock(Device.class);
      when(device.getGcmId()).thenReturn("fcm-token");

      arguments.add(Arguments.of(device, true));
    }

    {
      // APNs token
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(device, true));
    }

    return arguments;
  }

  @Test
  void getState() {
    final IdleDevicePushNotificationExperiment experiment = getExperiment();

    assertEquals(DeviceLastSeenState.MISSING_DEVICE_STATE, experiment.getState(null, null));
    assertEquals(DeviceLastSeenState.MISSING_DEVICE_STATE, experiment.getState(mock(Account.class), null));

    final int registrationId = 123;

    {
      final Device apnsDevice = mock(Device.class);
      when(apnsDevice.getApnId()).thenReturn("apns-token");
      when(apnsDevice.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
      when(apnsDevice.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      assertEquals(
          new DeviceLastSeenState(true, registrationId, true, CURRENT_TIME.toEpochMilli(), DeviceLastSeenState.PushTokenType.APNS),
          experiment.getState(mock(Account.class), apnsDevice));
    }

    {
      final Device fcmDevice = mock(Device.class);
      when(fcmDevice.getGcmId()).thenReturn("fcm-token");
      when(fcmDevice.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
      when(fcmDevice.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      assertEquals(
          new DeviceLastSeenState(true, registrationId, true, CURRENT_TIME.toEpochMilli(), DeviceLastSeenState.PushTokenType.FCM),
          experiment.getState(mock(Account.class), fcmDevice));
    }

    {
      final Device noTokenDevice = mock(Device.class);
      when(noTokenDevice.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
      when(noTokenDevice.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      assertEquals(
          new DeviceLastSeenState(true, registrationId, false, CURRENT_TIME.toEpochMilli(), null),
          experiment.getState(mock(Account.class), noTokenDevice));
    }
  }
  @ParameterizedTest
  @MethodSource
  void getPopulation(final boolean inExperimentGroup,
      final DeviceLastSeenState.PushTokenType tokenType,
      final IdleDevicePushNotificationExperiment.Population expectedPopulation) {

    final DeviceLastSeenState state = new DeviceLastSeenState(true, 0, true, 0, tokenType);
    final PushNotificationExperimentSample<DeviceLastSeenState> sample =
        new PushNotificationExperimentSample<>(UUID.randomUUID(), Device.PRIMARY_ID, inExperimentGroup, state, state);

    assertEquals(expectedPopulation, IdleDevicePushNotificationExperiment.getPopulation(sample));
  }

  private static List<Arguments> getPopulation() {
    return List.of(
        Arguments.of(true, DeviceLastSeenState.PushTokenType.APNS,
            IdleDevicePushNotificationExperiment.Population.APNS_EXPERIMENT),

        Arguments.of(false, DeviceLastSeenState.PushTokenType.APNS,
            IdleDevicePushNotificationExperiment.Population.APNS_CONTROL),

        Arguments.of(true, DeviceLastSeenState.PushTokenType.FCM,
            IdleDevicePushNotificationExperiment.Population.FCM_EXPERIMENT),

        Arguments.of(false, DeviceLastSeenState.PushTokenType.FCM,
            IdleDevicePushNotificationExperiment.Population.FCM_CONTROL)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getOutcome(final DeviceLastSeenState initialState,
      final DeviceLastSeenState finalState,
      final IdleDevicePushNotificationExperiment.Outcome expectedOutcome) {

    final PushNotificationExperimentSample<DeviceLastSeenState> sample =
        new PushNotificationExperimentSample<>(UUID.randomUUID(), Device.PRIMARY_ID, true, initialState, finalState);

    assertEquals(expectedOutcome, IdleDevicePushNotificationExperiment.getOutcome(sample));
  }

  private static List<Arguments> getOutcome() {
    return List.of(
        // Device no longer exists
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            DeviceLastSeenState.MISSING_DEVICE_STATE,
            IdleDevicePushNotificationExperiment.Outcome.DELETED
        ),

        // Device re-registered (i.e. registration ID changed)
        Arguments.of(
            new DeviceLastSeenState(true, 123, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 1234, true, 1, DeviceLastSeenState.PushTokenType.APNS),
            IdleDevicePushNotificationExperiment.Outcome.DELETED
        ),

        // Device has lost push tokens
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, false, 0, DeviceLastSeenState.PushTokenType.APNS),
            IdleDevicePushNotificationExperiment.Outcome.UNINSTALLED
        ),

        // Device reactivated
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, true, 1, DeviceLastSeenState.PushTokenType.APNS),
            IdleDevicePushNotificationExperiment.Outcome.REACTIVATED
        ),

        // No change
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            IdleDevicePushNotificationExperiment.Outcome.UNCHANGED
        )
    );
  }

  @Test
  void analyzeResults() {
    assertDoesNotThrow(() -> getExperiment().analyzeResults(
        Flux.just(new PushNotificationExperimentSample<>(UUID.randomUUID(), Device.PRIMARY_ID, true,
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS)))));
  }
}
