/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

class MessageSenderTest {

  private MessagesManager messagesManager;
  private PushNotificationManager pushNotificationManager;
  private MessageSender messageSender;

  @BeforeEach
  void setUp() {
    messagesManager = mock(MessagesManager.class);
    pushNotificationManager = mock(PushNotificationManager.class);

    messageSender = new MessageSender(messagesManager, pushNotificationManager);
  }

  @CartesianTest
  void sendMessage(@CartesianTest.Values(booleans = {true, false}) final boolean clientPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean ephemeral,
      @CartesianTest.Values(booleans = {true, false}) final boolean urgent,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasPushToken) throws NotPushRegisteredException {

    final boolean expectPushNotificationAttempt = !clientPresent && !ephemeral;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final MessageProtos.Envelope message = MessageProtos.Envelope.newBuilder()
        .setEphemeral(ephemeral)
        .setUrgent(urgent)
        .build();

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(device.getId()).thenReturn(deviceId);

    if (hasPushToken) {
      when(device.getApnId()).thenReturn("apns-token");
    } else {
      doThrow(NotPushRegisteredException.class)
          .when(pushNotificationManager).sendNewMessageNotification(any(), anyByte(), anyBoolean());
    }

    when(messagesManager.insert(any(), any())).thenReturn(Map.of(deviceId, clientPresent));

    assertDoesNotThrow(() -> messageSender.sendMessages(account, Map.of(device.getId(), message)));

    final MessageProtos.Envelope expectedMessage = ephemeral
        ? message.toBuilder().setEphemeral(true).build()
        : message.toBuilder().build();

    verify(messagesManager).insert(accountIdentifier, Map.of(deviceId, expectedMessage));

    if (expectPushNotificationAttempt) {
      verify(pushNotificationManager).sendNewMessageNotification(account, deviceId, urgent);
    } else {
      verifyNoInteractions(pushNotificationManager);
    }
  }

  @CartesianTest
  void sendMultiRecipientMessage(@CartesianTest.Values(booleans = {true, false}) final boolean clientPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean ephemeral,
      @CartesianTest.Values(booleans = {true, false}) final boolean urgent,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasPushToken) throws NotPushRegisteredException {

    final boolean expectPushNotificationAttempt = !clientPresent && !ephemeral;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(device.getId()).thenReturn(deviceId);

    if (hasPushToken) {
      when(device.getApnId()).thenReturn("apns-token");
    } else {
      doThrow(NotPushRegisteredException.class)
          .when(pushNotificationManager).sendNewMessageNotification(any(), anyByte(), anyBoolean());
    }

    when(messagesManager.insertMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(Map.of(account, Map.of(deviceId, clientPresent))));

    assertDoesNotThrow(() -> messageSender.sendMultiRecipientMessage(mock(SealedSenderMultiRecipientMessage.class),
        Collections.emptyMap(),
        System.currentTimeMillis(),
        false,
        ephemeral,
        urgent)
        .join());

    if (expectPushNotificationAttempt) {
      verify(pushNotificationManager).sendNewMessageNotification(account, deviceId, urgent);
    } else {
      verifyNoInteractions(pushNotificationManager);
    }
  }

  @ParameterizedTest
  @MethodSource
  void getDeliveryChannelName(final Device device, final String expectedChannelName) {
    assertEquals(expectedChannelName, MessageSender.getDeliveryChannelName(device));
  }

  private static List<Arguments> getDeliveryChannelName() {
    final List<Arguments> arguments = new ArrayList<>();

    {
      final Device apnDevice = mock(Device.class);
      when(apnDevice.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(apnDevice, "apn"));
    }

    {
      final Device fcmDevice = mock(Device.class);
      when(fcmDevice.getGcmId()).thenReturn("fcm-token");

      arguments.add(Arguments.of(fcmDevice, "gcm"));
    }

    {
      final Device fetchesMessagesDevice = mock(Device.class);
      when(fetchesMessagesDevice.getFetchesMessages()).thenReturn(true);

      arguments.add(Arguments.of(fetchesMessagesDevice, "websocket"));
    }

    arguments.add(Arguments.of(mock(Device.class), "none"));

    return arguments;
  }
}
