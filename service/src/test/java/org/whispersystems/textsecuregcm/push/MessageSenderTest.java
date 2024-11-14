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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
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
      @CartesianTest.Values(booleans = {true, false}) final boolean onlineMessage,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasPushToken) throws NotPushRegisteredException {

    final boolean expectPushNotificationAttempt = !clientPresent && !onlineMessage;

    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final MessageProtos.Envelope message = generateRandomMessage();

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(device.getId()).thenReturn(deviceId);

    if (hasPushToken) {
      when(device.getApnId()).thenReturn("apns-token");
    } else {
      doThrow(NotPushRegisteredException.class)
          .when(pushNotificationManager).sendNewMessageNotification(any(), anyByte(), anyBoolean());
    }

    when(messagesManager.insert(eq(accountIdentifier), eq(deviceId), any())).thenReturn(clientPresent);

    assertDoesNotThrow(() -> messageSender.sendMessage(account, device, message, onlineMessage));

    final MessageProtos.Envelope expectedMessage = onlineMessage
        ? message.toBuilder().setEphemeral(true).build()
        : message.toBuilder().build();

    verify(messagesManager).insert(accountIdentifier, deviceId, expectedMessage);

    if (expectPushNotificationAttempt) {
      verify(pushNotificationManager).sendNewMessageNotification(account, deviceId, expectedMessage.getUrgent());
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

  private MessageProtos.Envelope generateRandomMessage() {
    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(System.currentTimeMillis())
        .setServerTimestamp(System.currentTimeMillis())
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.secure().nextAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(UUID.randomUUID().toString())
        .build();
  }
}
