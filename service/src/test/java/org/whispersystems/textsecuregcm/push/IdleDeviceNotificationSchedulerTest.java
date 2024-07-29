package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

class IdleDeviceNotificationSchedulerTest {

  private AccountsManager accountsManager;
  private PushNotificationManager pushNotificationManager;

  private IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;

  private static final Instant CURRENT_TIME = Instant.now();

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    pushNotificationManager = mock(PushNotificationManager.class);

    idleDeviceNotificationScheduler = new IdleDeviceNotificationScheduler(
        accountsManager,
        pushNotificationManager,
        mock(DynamoDbAsyncClient.class),
        "test-idle-device-notifications",
        Duration.ofDays(7),
        Clock.fixed(CURRENT_TIME, ZoneId.systemDefault()));
  }

  @ParameterizedTest
  @MethodSource
  void processJob(final boolean accountPresent,
      final boolean devicePresent,
      final boolean tokenPresent,
      final Instant deviceLastSeen,
      final String expectedOutcome) throws JsonProcessingException, NotPushRegisteredException {

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Device device = mock(Device.class);
    when(device.getLastSeen()).thenReturn(deviceLastSeen.toEpochMilli());

    final Account account = mock(Account.class);
    when(account.getDevice(deviceId)).thenReturn(devicePresent ? Optional.of(device) : Optional.empty());

    when(accountsManager.getByAccountIdentifierAsync(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(accountPresent ? Optional.of(account) : Optional.empty()));

    if (tokenPresent) {
      when(pushNotificationManager.sendNewMessageNotification(any(), anyByte(), anyBoolean()))
          .thenReturn(CompletableFuture.completedFuture(
              Optional.of(new SendPushNotificationResult(true, Optional.empty(), false, Optional.empty()))));
    } else {
      when(pushNotificationManager.sendNewMessageNotification(any(), anyByte(), anyBoolean()))
          .thenThrow(NotPushRegisteredException.class);
    }

    final byte[] jobData = SystemMapper.jsonMapper().writeValueAsBytes(
        new IdleDeviceNotificationScheduler.AccountAndDeviceIdentifier(accountIdentifier, deviceId));

    assertEquals(expectedOutcome, idleDeviceNotificationScheduler.processJob(jobData).join());
  }

  private static List<Arguments> processJob() {
    final Instant idleDeviceLastSeenTimestamp = CURRENT_TIME
        .minus(IdleDeviceNotificationScheduler.MIN_IDLE_DURATION)
        .minus(Duration.ofDays(1));

    return List.of(
        // Account present, device present, device has tokens, device is idle
        Arguments.of(true, true, true, idleDeviceLastSeenTimestamp, "sent"),

        // Account present, device present, device has tokens, but device is active
        Arguments.of(true, true, true, CURRENT_TIME, "deviceSeenRecently"),

        // Account present, device present, device is idle, but missing tokens
        Arguments.of(true, true, false, idleDeviceLastSeenTimestamp, "deviceTokenDeleted"),

        // Account present, but device missing
        Arguments.of(true, false, true, idleDeviceLastSeenTimestamp, "deviceDeleted"),

        // Account missing
        Arguments.of(false, true, true, idleDeviceLastSeenTimestamp, "accountDeleted")
    );
  }

  @Test
  void isIdle() {
    {
      final Device idleDevice = mock(Device.class);
      when(idleDevice.getLastSeen())
          .thenReturn(CURRENT_TIME.minus(IdleDeviceNotificationScheduler.MIN_IDLE_DURATION).minus(Duration.ofDays(1))
              .toEpochMilli());

      assertTrue(idleDeviceNotificationScheduler.isIdle(idleDevice));
    }

    {
      final Device activeDevice = mock(Device.class);
      when(activeDevice.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      assertFalse(idleDeviceNotificationScheduler.isIdle(activeDevice));
    }
  }
}
