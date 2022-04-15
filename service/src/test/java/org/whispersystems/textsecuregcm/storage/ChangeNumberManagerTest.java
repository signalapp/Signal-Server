/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.push.MessageSender;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChangeNumberManagerTest {
  private static AccountsManager accountsManager = mock(AccountsManager.class);
  private static MessageSender messageSender = mock(MessageSender.class);
  private ChangeNumberManager changeNumberManager = new ChangeNumberManager(messageSender, accountsManager);

  @BeforeEach
  void reset() throws Exception {
    Mockito.reset(accountsManager, messageSender);
    when(accountsManager.changeNumber(any(), any())).thenAnswer((Answer<Account>) invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final String number = invocation.getArgument(1, String.class);

      final UUID uuid = account.getUuid();
      final Set<Device> devices = account.getDevices();

      final Account updatedAccount = mock(Account.class);
      when(updatedAccount.getUuid()).thenReturn(uuid);
      when(updatedAccount.getNumber()).thenReturn(number);
      when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(UUID.randomUUID());
      when(updatedAccount.getDevices()).thenReturn(devices);
      for (long i = 1; i <= 3; i++) {
        final Optional<Device> d = account.getDevice(i);
        when(updatedAccount.getDevice(i)).thenReturn(d);
      }

      return updatedAccount;
    });
  }

  @Test
  void changeNumberNoMessages() throws Exception {
    Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    changeNumberManager.changeNumber(account, "+18025551234", Collections.EMPTY_MAP, Collections.EMPTY_LIST);
    verify(accountsManager).changeNumber(account, "+18025551234");
    verify(accountsManager, never()).updateDevice(any(), eq(1L), any());
    verify(messageSender, never()).sendMessage(eq(account), any(), any(), eq(false));
  }

  @Test
  void changeNumberSetPrimaryDevicePrekey() throws Exception {
    Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    var prekeys = Map.of(1L, new SignedPreKey());
    changeNumberManager.changeNumber(account, "+18025551234", prekeys, Collections.EMPTY_LIST);
    verify(accountsManager).changeNumber(account, "+18025551234");
    verify(accountsManager).updateDevice(any(), eq(1L), any());
    verify(messageSender, never()).sendMessage(eq(account), any(), any(), eq(false));
  }

  @Test
  void changeNumberSetPrimaryDevicePrekeyAndSendMessages() throws Exception {
    Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    Device d2 = mock(Device.class);
    when(account.getDevice(2L)).thenReturn(Optional.of(d2));
    var prekeys = Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey());
    IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.getDestinationDeviceId()).thenReturn(2L);
    when(msg.getContent()).thenReturn(Base64.encodeBase64String(new byte[]{1}));
    changeNumberManager.changeNumber(account, "+18025551234", prekeys, List.of(msg));
    verify(accountsManager).changeNumber(account, "+18025551234");
    verify(accountsManager).updateDevice(any(), eq(1L), any());
    verify(accountsManager).updateDevice(any(), eq(2L), any());
    verify(messageSender).sendMessage(any(), eq(d2), any(), eq(false));
  }
}
