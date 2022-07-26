/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.push.MessageSender;

public class ChangeNumberManagerTest {
  private AccountsManager accountsManager;
  private MessageSender messageSender;
  private ChangeNumberManager changeNumberManager;

  private Map<Account, UUID> updatedPhoneNumberIdentifiersByAccount;

  @BeforeEach
  void setUp() throws Exception {
    accountsManager = mock(AccountsManager.class);
    messageSender = mock(MessageSender.class);
    changeNumberManager = new ChangeNumberManager(messageSender, accountsManager);

    updatedPhoneNumberIdentifiersByAccount = new HashMap<>();

    when(accountsManager.changeNumber(any(), any(), any(), any(), any())).thenAnswer((Answer<Account>)invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final String number = invocation.getArgument(1, String.class);

      final UUID uuid = account.getUuid();
      final List<Device> devices = account.getDevices();

      final UUID updatedPni = UUID.randomUUID();
      updatedPhoneNumberIdentifiersByAccount.put(account, updatedPni);

      final Account updatedAccount = mock(Account.class);
      when(updatedAccount.getUuid()).thenReturn(uuid);
      when(updatedAccount.getNumber()).thenReturn(number);
      when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(updatedPni);
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
    changeNumberManager.changeNumber(account, "+18025551234", null, null, null, null);
    verify(accountsManager).changeNumber(account, "+18025551234", null, null, null);
    verify(accountsManager, never()).updateDevice(any(), eq(1L), any());
    verify(messageSender, never()).sendMessage(eq(account), any(), any(), eq(false));
  }

  @Test
  void changeNumberSetPrimaryDevicePrekey() throws Exception {
    Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    var prekeys = Map.of(1L, new SignedPreKey());
    final String pniIdentityKey = "pni-identity-key";

    changeNumberManager.changeNumber(account, "+18025551234", pniIdentityKey, prekeys, Collections.emptyList(), Collections.emptyMap());
    verify(accountsManager).changeNumber(account, "+18025551234", pniIdentityKey, prekeys, Collections.emptyMap());
    verify(messageSender, never()).sendMessage(eq(account), any(), any(), eq(false));
  }

  @Test
  void changeNumberSetPrimaryDevicePrekeyAndSendMessages() throws Exception {
    final String originalE164 = "+18005551234";
    final String changedE164 = "+18025551234";
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalE164);
    when(account.getUuid()).thenReturn(aci);
    when(account.getPhoneNumberIdentifier()).thenReturn(pni);

    final Device d2 = mock(Device.class);
    when(d2.isEnabled()).thenReturn(true);
    when(d2.getId()).thenReturn(2L);

    when(account.getDevice(2L)).thenReturn(Optional.of(d2));
    when(account.getDevices()).thenReturn(List.of(d2));

    final String pniIdentityKey = "pni-identity-key";
    final Map<Long, SignedPreKey> prekeys = Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey());
    final Map<Long, Integer> registrationIds = Map.of(1L, 17, 2L, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.getDestinationDeviceId()).thenReturn(2L);
    when(msg.getContent()).thenReturn(Base64.encodeBase64String(new byte[]{1}));

    changeNumberManager.changeNumber(account, changedE164, pniIdentityKey, prekeys, List.of(msg), registrationIds);

    verify(accountsManager).changeNumber(account, changedE164, pniIdentityKey, prekeys, registrationIds);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeCaptor = ArgumentCaptor.forClass(MessageProtos.Envelope.class);
    verify(messageSender).sendMessage(any(), eq(d2), envelopeCaptor.capture(), eq(false));

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue();

    assertEquals(aci, UUID.fromString(envelope.getDestinationUuid()));
    assertEquals(aci, UUID.fromString(envelope.getSourceUuid()));
    assertEquals(changedE164, envelope.getSource());
    assertEquals(Device.MASTER_ID, envelope.getSourceDevice());
    assertEquals(updatedPhoneNumberIdentifiersByAccount.get(account), UUID.fromString(envelope.getUpdatedPni()));
  }

  @Test
  void changeNumberMismatchedRegistrationId() {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");

    final List<Device> devices = new ArrayList<>();

    for (int i = 1; i <= 3; i++) {
      final Device device = mock(Device.class);
      when(device.getId()).thenReturn((long) i);
      when(device.isEnabled()).thenReturn(true);
      when(device.getRegistrationId()).thenReturn(i);

      devices.add(device);
      when(account.getDevice(i)).thenReturn(Optional.of(device));
    }

    when(account.getDevices()).thenReturn(devices);

    final List<IncomingMessage> messages = List.of(
        new IncomingMessage(1, null, 2, 1, "foo"),
        new IncomingMessage(1, null, 3, 1, "foo"));

    final Map<Long, SignedPreKey> preKeys = Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey(), 3L, new SignedPreKey());
    final Map<Long, Integer> registrationIds = Map.of(1L, 17, 2L, 47, 3L, 89);

    assertThrows(StaleDevicesException.class,
        () -> changeNumberManager.changeNumber(account, "+18005559876", "pni-identity-key", preKeys, messages, registrationIds));
  }

  @Test
  void changeNumberMissingData() {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");

    final List<Device> devices = new ArrayList<>();

    for (int i = 1; i <= 3; i++) {
      final Device device = mock(Device.class);
      when(device.getId()).thenReturn((long) i);
      when(device.isEnabled()).thenReturn(true);
      when(device.getRegistrationId()).thenReturn(i);

      devices.add(device);
      when(account.getDevice(i)).thenReturn(Optional.of(device));
    }

    when(account.getDevices()).thenReturn(devices);

    final List<IncomingMessage> messages = List.of(
        new IncomingMessage(1, null, 2, 2, "foo"),
        new IncomingMessage(1, null, 3, 3, "foo"));

    final Map<Long, Integer> registrationIds = Map.of(1L, 17, 2L, 47, 3L, 89);

    assertThrows(IllegalArgumentException.class,
        () -> changeNumberManager.changeNumber(account, "+18005559876", "pni-identity-key", null, messages, registrationIds));
  }
}
