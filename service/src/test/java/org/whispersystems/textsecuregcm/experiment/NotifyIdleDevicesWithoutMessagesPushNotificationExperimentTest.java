package org.whispersystems.textsecuregcm.experiment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

class NotifyIdleDevicesWithoutMessagesPushNotificationExperimentTest {

  private MessagesManager messagesManager;
  private IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;

  private NotifyIdleDevicesWithoutMessagesPushNotificationExperiment experiment;

  private static final Instant CURRENT_TIME = Instant.now();

  @BeforeEach
  void setUp() {
    messagesManager = mock(MessagesManager.class);

    idleDeviceNotificationScheduler = mock(IdleDeviceNotificationScheduler.class);
    when(idleDeviceNotificationScheduler.scheduleNotification(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    experiment = new NotifyIdleDevicesWithoutMessagesPushNotificationExperiment(messagesManager,
        idleDeviceNotificationScheduler);
  }

  @ParameterizedTest
  @MethodSource
  void isDeviceEligible(final Account account,
      final Device device,
      final boolean isDeviceIdle,
      final boolean mayHaveMessages,
      final boolean expectEligible) {

    when(messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(CompletableFuture.completedFuture(mayHaveMessages));

    when(idleDeviceNotificationScheduler.isIdle(device)).thenReturn(isDeviceIdle);

    assertEquals(expectEligible, experiment.isDeviceEligible(account, device).join());
  }

  private static List<Arguments> isDeviceEligible() {
    final List<Arguments> arguments = new ArrayList<>();

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164));

    {
      // Idle device with push token and messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Idle device missing push token, but with messages
      arguments.add(Arguments.of(account, mock(Device.class), true, true, false));
    }

    {
      // Idle device missing push token and messages
      arguments.add(Arguments.of(account, mock(Device.class), true, false, false));
    }

    {
      // Idle device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, true, false, true));
    }

    {
      // Active device with push token and messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, true, false));
    }

    {
      // Active device missing push token, but with messages
      arguments.add(Arguments.of(account, mock(Device.class), false, true, false));
    }

    {
      // Active device missing push token and messages
      arguments.add(Arguments.of(account, mock(Device.class), false, false, false));
    }

    {
      // Active device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  void hasPushToken(final Device device, final boolean expectHasPushToken) {
    assertEquals(expectHasPushToken, NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.hasPushToken(device));
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

    {
      // APNs VOIP token
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getVoipApnId()).thenReturn("apns-voip-token");

      arguments.add(Arguments.of(device, false));
    }

    return arguments;
  }

  @Test
  void getState() {
    assertEquals(DeviceLastSeenState.MISSING_DEVICE_STATE, experiment.getState(null, null));
    assertEquals(DeviceLastSeenState.MISSING_DEVICE_STATE, experiment.getState(mock(Account.class), null));

    final long createdAtMillis = CURRENT_TIME.minus(Duration.ofDays(14)).toEpochMilli();

    {
      final Device apnsDevice = mock(Device.class);
      when(apnsDevice.getApnId()).thenReturn("apns-token");
      when(apnsDevice.getCreated()).thenReturn(createdAtMillis);
      when(apnsDevice.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      assertEquals(
          new DeviceLastSeenState(true, createdAtMillis, true, CURRENT_TIME.toEpochMilli(), DeviceLastSeenState.PushTokenType.APNS),
          experiment.getState(mock(Account.class), apnsDevice));
    }

    {
      final Device fcmDevice = mock(Device.class);
      when(fcmDevice.getGcmId()).thenReturn("fcm-token");
      when(fcmDevice.getCreated()).thenReturn(createdAtMillis);
      when(fcmDevice.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      assertEquals(
          new DeviceLastSeenState(true, createdAtMillis, true, CURRENT_TIME.toEpochMilli(), DeviceLastSeenState.PushTokenType.FCM),
          experiment.getState(mock(Account.class), fcmDevice));
    }
  }

  @ParameterizedTest
  @MethodSource
  void getPopulation(final boolean inExperimentGroup,
      final DeviceLastSeenState.PushTokenType tokenType,
      final NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Population expectedPopulation) {

    final DeviceLastSeenState state = new DeviceLastSeenState(true, 0, true, 0, tokenType);
    final PushNotificationExperimentSample<DeviceLastSeenState> sample =
        new PushNotificationExperimentSample<>(inExperimentGroup, state, state);

    assertEquals(expectedPopulation, NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.getPopulation(sample));
  }

  private static List<Arguments> getPopulation() {
    return List.of(
        Arguments.of(true, DeviceLastSeenState.PushTokenType.APNS,
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Population.APNS_EXPERIMENT),

        Arguments.of(false, DeviceLastSeenState.PushTokenType.APNS,
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Population.APNS_CONTROL),

        Arguments.of(true, DeviceLastSeenState.PushTokenType.FCM,
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Population.FCM_EXPERIMENT),

        Arguments.of(false, DeviceLastSeenState.PushTokenType.FCM,
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Population.FCM_CONTROL)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getOutcome(final DeviceLastSeenState initialState,
      final DeviceLastSeenState finalState,
      final NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Outcome expectedOutcome) {

    final PushNotificationExperimentSample<DeviceLastSeenState> sample =
        new PushNotificationExperimentSample<>(true, initialState, finalState);

    assertEquals(expectedOutcome, NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.getOutcome(sample));
  }

  private static List<Arguments> getOutcome() {
    return List.of(
        // Device no longer exists
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            DeviceLastSeenState.MISSING_DEVICE_STATE,
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Outcome.DELETED
        ),

        // Device re-registered (i.e. "created" timestamp changed)
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 1, true, 1, DeviceLastSeenState.PushTokenType.APNS),
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Outcome.DELETED
        ),

        // Device has lost push tokens
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, false, 0, DeviceLastSeenState.PushTokenType.APNS),
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Outcome.UNINSTALLED
        ),

        // Device reactivated
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, true, 1, DeviceLastSeenState.PushTokenType.APNS),
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Outcome.REACTIVATED
        ),

        // No change
        Arguments.of(
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            new DeviceLastSeenState(true, 0, true, 0, DeviceLastSeenState.PushTokenType.APNS),
            NotifyIdleDevicesWithoutMessagesPushNotificationExperiment.Outcome.UNCHANGED
        )
    );
  }
}
