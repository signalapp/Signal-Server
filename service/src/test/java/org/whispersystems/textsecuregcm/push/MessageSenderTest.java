/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.metrics.PushLatencyManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class MessageSenderTest {

    private Account                account;
    private Device                 device;
    private MessageProtos.Envelope message;

    private ClientPresenceManager clientPresenceManager;
    private MessagesManager       messagesManager;
    private GCMSender             gcmSender;
    private APNSender             apnSender;
    private MessageSender         messageSender;

    private static final UUID ACCOUNT_UUID = UUID.randomUUID();
    private static final long DEVICE_ID = 1L;

    @Before
    public void setUp() {

        account = mock(Account.class);
        device  = mock(Device.class);
        message = generateRandomMessage();

        clientPresenceManager = mock(ClientPresenceManager.class);
        messagesManager       = mock(MessagesManager.class);
        gcmSender             = mock(GCMSender.class);
        apnSender             = mock(APNSender.class);
        messageSender         = new MessageSender(mock(ApnFallbackManager.class),
                                                  clientPresenceManager,
                                                  messagesManager,
                                                  gcmSender,
                                                  apnSender,
                                                  mock(PushLatencyManager.class));

        when(account.getUuid()).thenReturn(ACCOUNT_UUID);
        when(device.getId()).thenReturn(DEVICE_ID);
    }

    @Test
    public void testSendOnlineMessageClientPresent() throws Exception {
        when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(true);
        when(device.getGcmId()).thenReturn("gcm-id");

        messageSender.sendMessage(account, device, message, true);

        verify(messagesManager).insertEphemeral(ACCOUNT_UUID, DEVICE_ID, message);
        verify(messagesManager, never()).insert(any(), anyLong(), any());
        verifyZeroInteractions(gcmSender);
        verifyZeroInteractions(apnSender);
    }

    @Test
    public void testSendOnlineMessageClientNotPresent() throws Exception {
        when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
        when(device.getGcmId()).thenReturn("gcm-id");

        messageSender.sendMessage(account, device, message, true);

        verify(messagesManager, never()).insertEphemeral(any(), anyLong(), any());
        verify(messagesManager, never()).insert(any(), anyLong(), any());
        verifyZeroInteractions(gcmSender);
        verifyZeroInteractions(apnSender);
    }

    @Test
    public void testSendMessageClientPresent() throws Exception {
        when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(true);
        when(device.getGcmId()).thenReturn("gcm-id");

        messageSender.sendMessage(account, device, message, false);

        verify(messagesManager, never()).insertEphemeral(any(), anyLong(), any());
        verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
        verifyZeroInteractions(gcmSender);
        verifyZeroInteractions(apnSender);
    }

    @Test
    public void testSendMessageGcmClientNotPresent() throws Exception {
        when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
        when(device.getGcmId()).thenReturn("gcm-id");

        messageSender.sendMessage(account, device, message, false);

        verify(messagesManager, never()).insertEphemeral(any(), anyLong(), any());
        verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
        verify(gcmSender).sendMessage(any());
        verifyZeroInteractions(apnSender);
    }

    @Test
    public void testSendMessageApnClientNotPresent() throws Exception {
        when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
        when(device.getApnId()).thenReturn("apn-id");

        messageSender.sendMessage(account, device, message, false);

        verify(messagesManager, never()).insertEphemeral(any(), anyLong(), any());
        verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
        verifyZeroInteractions(gcmSender);
        verify(apnSender).sendMessage(any());
    }

    @Test
    public void testSendMessageFetchClientNotPresent() throws Exception {
        when(clientPresenceManager.isPresent(ACCOUNT_UUID, DEVICE_ID)).thenReturn(false);
        when(device.getFetchesMessages()).thenReturn(true);

        messageSender.sendMessage(account, device, message, false);

        verify(messagesManager, never()).insertEphemeral(any(), anyLong(), any());
        verify(messagesManager).insert(ACCOUNT_UUID, DEVICE_ID, message);
        verifyZeroInteractions(gcmSender);
        verifyZeroInteractions(apnSender);
    }

    private MessageProtos.Envelope generateRandomMessage() {
        return MessageProtos.Envelope.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setServerTimestamp(System.currentTimeMillis())
                .setContent(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(256)))
                .setType(MessageProtos.Envelope.Type.CIPHERTEXT)
                .setServerGuid(UUID.randomUUID().toString())
                .build();
    }
}
