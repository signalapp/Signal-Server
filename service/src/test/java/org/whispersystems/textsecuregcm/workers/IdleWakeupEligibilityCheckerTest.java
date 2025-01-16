/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

public class IdleWakeupEligibilityCheckerTest {

  private static final Instant CURRENT_TIME = Instant.now();
  private final Clock clock = Clock.fixed(CURRENT_TIME, ZoneId.systemDefault());

  private MessagesManager messagesManager;
  private IdleWakeupEligibilityChecker idleChecker;

  @BeforeEach
  void setup() {
    messagesManager = mock(MessagesManager.class);
    idleChecker = new IdleWakeupEligibilityChecker(clock, messagesManager);
  }

  @ParameterizedTest
  @MethodSource
  void isDeviceEligible(final Account account,
      final Device device,
      final boolean mayHaveMessages,
      final boolean mayHaveUrgentMessages,
      final boolean expectEligible) {

    when(messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(CompletableFuture.completedFuture(mayHaveMessages));

    when(messagesManager.mayHaveUrgentPersistedMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(CompletableFuture.completedFuture(mayHaveUrgentMessages));

    assertEquals(expectEligible, idleChecker.isDeviceEligible(account, device).join());
  }

  private static List<Arguments> isDeviceEligible() {
    final List<Arguments> arguments = new ArrayList<>();

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164));

    {
      // Long-idle device with push token and messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Long-idle device missing push token, but with messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Long-idle device missing push token and messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Long-idle device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION).toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, true));
    }

    {
      // Short-idle device with push token and urgent messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, true));
    }

    {
      // Short-idle device with push token and only non-urgent messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, false, false));
    }

    {
      // Short-idle device missing push token, but with urgent messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Short-idle device missing push token and messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Short-idle device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(
          CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION).toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Active device with push token and urgent messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Active device missing push token, but with urgent messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Active device missing push token and messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Active device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  void isShortIdle(final Duration idleDuration, final boolean expectIdle) {
    final Instant currentTime = Instant.now();
    final Clock clock = Clock.fixed(currentTime, ZoneId.systemDefault());

    final Device device = mock(Device.class);
    when(device.getLastSeen()).thenReturn(currentTime.minus(idleDuration).toEpochMilli());

    assertEquals(expectIdle, IdleWakeupEligibilityChecker.isShortIdle(device, clock));
  }

  private static List<Arguments> isShortIdle() {
    return List.of(
        Arguments.of(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION, true),
        Arguments.of(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION.plusMillis(1), true),
        Arguments.of(IdleWakeupEligibilityChecker.MIN_SHORT_IDLE_DURATION.minusMillis(1), false),
        Arguments.of(IdleWakeupEligibilityChecker.MAX_SHORT_IDLE_DURATION, false),
        Arguments.of(IdleWakeupEligibilityChecker.MAX_SHORT_IDLE_DURATION.plusMillis(1), false),
        Arguments.of(IdleWakeupEligibilityChecker.MAX_SHORT_IDLE_DURATION.minusMillis(1), true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void isLongIdle(final Duration idleDuration, final boolean expectIdle) {
    final Instant currentTime = Instant.now();
    final Clock clock = Clock.fixed(currentTime, ZoneId.systemDefault());

    final Device device = mock(Device.class);
    when(device.getLastSeen()).thenReturn(currentTime.minus(idleDuration).toEpochMilli());

    assertEquals(expectIdle, IdleWakeupEligibilityChecker.isLongIdle(device, clock));
  }

  private static List<Arguments> isLongIdle() {
    return List.of(
        Arguments.of(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION, true),
        Arguments.of(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION.plusMillis(1), true),
        Arguments.of(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION.minusMillis(1), false),
        Arguments.of(IdleWakeupEligibilityChecker.MAX_LONG_IDLE_DURATION, false),
        Arguments.of(IdleWakeupEligibilityChecker.MAX_LONG_IDLE_DURATION.plusMillis(1), false),
        Arguments.of(IdleWakeupEligibilityChecker.MAX_LONG_IDLE_DURATION.minusMillis(1), true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void hasPushToken(final Device device, final boolean expectHasPushToken) {
    assertEquals(expectHasPushToken, IdleWakeupEligibilityChecker.hasPushToken(device));
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
}
