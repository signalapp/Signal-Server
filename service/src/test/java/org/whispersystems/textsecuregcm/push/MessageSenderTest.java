/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

class MessageSenderTest {

  private Account account;
  private Device device;
  private MessageProtos.Envelope message;

  private ClientPresenceManager clientPresenceManager;
  private PubSubClientEventManager pubSubClientEventManager;
  private MessagesManager messagesManager;
  private PushNotificationManager pushNotificationManager;
  private MessageSender messageSender;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final byte DEVICE_ID = 1;

  @BeforeEach
  void setUp() {

    account = mock(Account.class);
    device = mock(Device.class);
    message = generateRandomMessage();

    clientPresenceManager = mock(ClientPresenceManager.class);
    pubSubClientEventManager = mock(PubSubClientEventManager.class);
    messagesManager = mock(MessagesManager.class);
    pushNotificationManager = mock(PushNotificationManager.class);

    when(pubSubClientEventManager.handleNewMessageAvailable(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(true));

    messageSender = new MessageSender(clientPresenceManager, pubSubClientEventManager, messagesManager, pushNotificationManager);

    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
    when(device.getId()).thenReturn(DEVICE_ID);
  }

  @Test
  void testSendOnlineMessageClientPresent() throws Exception {
    when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(true);
    when(device.getGcmId()).thenReturn("gcm-id");

    messageSender.sendMessage(account, device, message, true);

    ArgumentCaptor<MessageProtos.Envelope> envelopeArgumentCaptor = ArgumentCaptor.forClass(
        MessageProtos.Envelope.class);

    verify(messagesManager).insert(any(), anyByte(), envelopeArgumentCaptor.capture());
    verify(messagesManager, never()).removeRecipientViewFromMrmData(anyByte(), any(MessageProtos.Envelope.class));

    assertTrue(envelopeArgumentCaptor.getValue().getEphemeral());

    verifyNoInteractions(pushNotificationManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSendOnlineMessageClientNotPresent(final boolean hasSharedMrmKey) throws Exception {

    when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
    when(device.getGcmId()).thenReturn("gcm-id");

    if (hasSharedMrmKey) {
      messageSender.sendMessage(account, device,
          message.toBuilder().setSharedMrmKey(ByteString.copyFromUtf8("sharedMrmKey")).build(), true);
    } else {
      messageSender.sendMessage(account, device, message, true);
    }

    verify(messagesManager, never()).insert(any(), anyByte(), any());
    verify(messagesManager).removeRecipientViewFromMrmData(anyByte(), any(MessageProtos.Envelope.class));

    verifyNoInteractions(pushNotificationManager);
  }

  @Test
  void testSendMessageClientPresent() throws Exception {
    when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(true);
    when(device.getGcmId()).thenReturn("gcm-id");

    messageSender.sendMessage(account, device, message, false);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeArgumentCaptor = ArgumentCaptor.forClass(
        MessageProtos.Envelope.class);

    verify(messagesManager).insert(eq(ACCOUNT_UUID), eq(DEVICE_ID), envelopeArgumentCaptor.capture());

    assertFalse(envelopeArgumentCaptor.getValue().getEphemeral());
    assertEquals(message, envelopeArgumentCaptor.getValue());
    verifyNoInteractions(pushNotificationManager);
  }

  @Test
  void testSendMessageGcmClientNotPresent() throws Exception {
    when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
    when(device.getGcmId()).thenReturn("gcm-id");

    messageSender.sendMessage(account, device, message, false);

    verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
    verify(pushNotificationManager).sendNewMessageNotification(account, device.getId(), message.getUrgent());
  }

  @Test
  void testSendMessageApnClientNotPresent() throws Exception {
    when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
    when(device.getApnId()).thenReturn("apn-id");

    messageSender.sendMessage(account, device, message, false);

    verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
    verify(pushNotificationManager).sendNewMessageNotification(account, device.getId(), message.getUrgent());
  }

  @Test
  void testSendMessageFetchClientNotPresent() throws Exception {
    when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
    when(device.getFetchesMessages()).thenReturn(true);

    doThrow(NotPushRegisteredException.class)
        .when(pushNotificationManager).sendNewMessageNotification(account, DEVICE_ID, message.getUrgent());

    assertDoesNotThrow(() -> messageSender.sendMessage(account, device, message, false));
    verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
  }

  @Test
  void testSendMessageNoChannel() {
    when(device.getGcmId()).thenReturn(null);
    when(device.getApnId()).thenReturn(null);
    when(device.getFetchesMessages()).thenReturn(false);

    assertDoesNotThrow(() -> messageSender.sendMessage(account, device, message, false));
    verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
  }

  private MessageProtos.Envelope generateRandomMessage() {
    return MessageProtos.Envelope.newBuilder()
        .setClientTimestamp(System.currentTimeMillis())
        .setServerTimestamp(System.currentTimeMillis())
        .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
        .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
        .setServerGuid(UUID.randomUUID().toString())
        .build();
  }
}
