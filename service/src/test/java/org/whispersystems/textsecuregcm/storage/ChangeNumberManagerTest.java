/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

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

    when(accountsManager.changeNumber(any(), any(), any(), any(), any(), any())).thenAnswer((Answer<Account>)invocation -> {
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
      for (byte i = 1; i <= 3; i++) {
        final Optional<Device> d = account.getDevice(i);
        when(updatedAccount.getDevice(i)).thenReturn(d);
      }

      return updatedAccount;
    });

    when(accountsManager.updatePniKeys(any(), any(), any(), any(), any())).thenAnswer((Answer<Account>)invocation -> {
      final Account account = invocation.getArgument(0, Account.class);

      final UUID uuid = account.getUuid();
      final UUID pni = account.getPhoneNumberIdentifier();
      final List<Device> devices = account.getDevices();

      final Account updatedAccount = mock(Account.class);
      when(updatedAccount.getUuid()).thenReturn(uuid);
      when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(pni);
      when(updatedAccount.getDevices()).thenReturn(devices);
      for (byte i = 1; i <= 3; i++) {
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
    changeNumberManager.changeNumber(account, "+18025551234", null, null, null, null, null);
    verify(accountsManager).changeNumber(account, "+18025551234", null, null, null, null);
    verify(accountsManager, never()).updateDevice(any(), anyByte(), any());
    verify(messageSender, never()).sendMessage(eq(account), any(), any(), eq(false));
  }

  @Test
  void changeNumberSetPrimaryDevicePrekey() throws Exception {
    Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    changeNumberManager.changeNumber(account, "+18025551234", pniIdentityKey, prekeys, null, Collections.emptyList(), Collections.emptyMap());
    verify(accountsManager).changeNumber(account, "+18025551234", pniIdentityKey, prekeys, null, Collections.emptyMap());
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
    final byte deviceId2 = 2;
    when(d2.getId()).thenReturn(deviceId2);

    when(account.getDevice(deviceId2)).thenReturn(Optional.of(d2));
    when(account.getDevices()).thenReturn(List.of(d2));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, deviceId2, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.destinationDeviceId()).thenReturn(deviceId2);
    when(msg.content()).thenReturn(Base64.getEncoder().encodeToString(new byte[]{1}));

    changeNumberManager.changeNumber(account, changedE164, pniIdentityKey, prekeys, null, List.of(msg), registrationIds);

    verify(accountsManager).changeNumber(account, changedE164, pniIdentityKey, prekeys, null, registrationIds);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeCaptor = ArgumentCaptor.forClass(MessageProtos.Envelope.class);
    verify(messageSender).sendMessage(any(), eq(d2), envelopeCaptor.capture(), eq(false));

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue();

    assertEquals(aci, UUID.fromString(envelope.getDestinationUuid()));
    assertEquals(aci, UUID.fromString(envelope.getSourceUuid()));
    assertEquals(Device.PRIMARY_ID, envelope.getSourceDevice());
    assertEquals(updatedPhoneNumberIdentifiersByAccount.get(account), UUID.fromString(envelope.getUpdatedPni()));
  }


  @Test
  void changeNumberSetPrimaryDevicePrekeyPqAndSendMessages() throws Exception {
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
    final byte deviceId2 = 2;
    when(d2.getId()).thenReturn(deviceId2);

    when(account.getDevice(deviceId2)).thenReturn(Optional.of(d2));
    when(account.getDevices()).thenReturn(List.of(d2));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Map<Byte, KEMSignedPreKey> pqPrekeys = Map.of((byte) 3, KeysHelper.signedKEMPreKey(3, pniIdentityKeyPair),
        (byte) 4, KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, deviceId2, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.destinationDeviceId()).thenReturn(deviceId2);
    when(msg.content()).thenReturn(Base64.getEncoder().encodeToString(new byte[]{1}));

    changeNumberManager.changeNumber(account, changedE164, pniIdentityKey, prekeys, pqPrekeys, List.of(msg), registrationIds);

    verify(accountsManager).changeNumber(account, changedE164, pniIdentityKey, prekeys, pqPrekeys, registrationIds);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeCaptor = ArgumentCaptor.forClass(MessageProtos.Envelope.class);
    verify(messageSender).sendMessage(any(), eq(d2), envelopeCaptor.capture(), eq(false));

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue();

    assertEquals(aci, UUID.fromString(envelope.getDestinationUuid()));
    assertEquals(aci, UUID.fromString(envelope.getSourceUuid()));
    assertEquals(Device.PRIMARY_ID, envelope.getSourceDevice());
    assertEquals(updatedPhoneNumberIdentifiersByAccount.get(account), UUID.fromString(envelope.getUpdatedPni()));
  }

  @Test
  void changeNumberSameNumberSetPrimaryDevicePrekeyAndSendMessages() throws Exception {
    final String originalE164 = "+18005551234";
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalE164);
    when(account.getUuid()).thenReturn(aci);
    when(account.getPhoneNumberIdentifier()).thenReturn(pni);

    final Device d2 = mock(Device.class);
    when(d2.isEnabled()).thenReturn(true);
    final byte deviceId2 = 2;
    when(d2.getId()).thenReturn(deviceId2);

    when(account.getDevice(deviceId2)).thenReturn(Optional.of(d2));
    when(account.getDevices()).thenReturn(List.of(d2));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Map<Byte, KEMSignedPreKey> pqPrekeys = Map.of((byte) 3, KeysHelper.signedKEMPreKey(3, pniIdentityKeyPair),
        (byte) 4, KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, deviceId2, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.destinationDeviceId()).thenReturn(deviceId2);
    when(msg.content()).thenReturn(Base64.getEncoder().encodeToString(new byte[]{1}));

    changeNumberManager.changeNumber(account, originalE164, pniIdentityKey, prekeys, pqPrekeys, List.of(msg), registrationIds);

    verify(accountsManager).updatePniKeys(account, pniIdentityKey, prekeys, pqPrekeys, registrationIds);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeCaptor = ArgumentCaptor.forClass(MessageProtos.Envelope.class);
    verify(messageSender).sendMessage(any(), eq(d2), envelopeCaptor.capture(), eq(false));

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue();

    assertEquals(aci, UUID.fromString(envelope.getDestinationUuid()));
    assertEquals(aci, UUID.fromString(envelope.getSourceUuid()));
    assertEquals(Device.PRIMARY_ID, envelope.getSourceDevice());
    assertFalse(updatedPhoneNumberIdentifiersByAccount.containsKey(account));
  }

  @Test
  void updatePniKeysSetPrimaryDevicePrekeyAndSendMessages() throws Exception {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getPhoneNumberIdentifier()).thenReturn(pni);

    final Device d2 = mock(Device.class);
    when(d2.isEnabled()).thenReturn(true);
    final byte deviceId2 = 2;
    when(d2.getId()).thenReturn(deviceId2);

    when(account.getDevice(deviceId2)).thenReturn(Optional.of(d2));
    when(account.getDevices()).thenReturn(List.of(d2));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, deviceId2, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.destinationDeviceId()).thenReturn(deviceId2);
    when(msg.content()).thenReturn(Base64.getEncoder().encodeToString(new byte[]{1}));

    changeNumberManager.updatePniKeys(account, pniIdentityKey, prekeys, null, List.of(msg), registrationIds);

    verify(accountsManager).updatePniKeys(account, pniIdentityKey, prekeys, null, registrationIds);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeCaptor = ArgumentCaptor.forClass(MessageProtos.Envelope.class);
    verify(messageSender).sendMessage(any(), eq(d2), envelopeCaptor.capture(), eq(false));

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue();

    assertEquals(aci, UUID.fromString(envelope.getDestinationUuid()));
    assertEquals(aci, UUID.fromString(envelope.getSourceUuid()));
    assertEquals(Device.PRIMARY_ID, envelope.getSourceDevice());
    assertFalse(updatedPhoneNumberIdentifiersByAccount.containsKey(account));
  }

  @Test
  void updatePniKeysSetPrimaryDevicePrekeyPqAndSendMessages() throws Exception {
    final UUID aci = UUID.randomUUID();
    final UUID pni = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getPhoneNumberIdentifier()).thenReturn(pni);

    final Device d2 = mock(Device.class);
    when(d2.isEnabled()).thenReturn(true);
    final byte deviceId2 = 2;
    when(d2.getId()).thenReturn(deviceId2);

    when(account.getDevice(deviceId2)).thenReturn(Optional.of(d2));
    when(account.getDevices()).thenReturn(List.of(d2));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        deviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Map<Byte, KEMSignedPreKey> pqPrekeys = Map.of((byte) 3, KeysHelper.signedKEMPreKey(3, pniIdentityKeyPair),
        (byte) 4, KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, deviceId2, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.destinationDeviceId()).thenReturn(deviceId2);
    when(msg.content()).thenReturn(Base64.getEncoder().encodeToString(new byte[]{1}));

    changeNumberManager.updatePniKeys(account, pniIdentityKey, prekeys, pqPrekeys, List.of(msg), registrationIds);

    verify(accountsManager).updatePniKeys(account, pniIdentityKey, prekeys, pqPrekeys, registrationIds);

    final ArgumentCaptor<MessageProtos.Envelope> envelopeCaptor = ArgumentCaptor.forClass(MessageProtos.Envelope.class);
    verify(messageSender).sendMessage(any(), eq(d2), envelopeCaptor.capture(), eq(false));

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue();

    assertEquals(aci, UUID.fromString(envelope.getDestinationUuid()));
    assertEquals(aci, UUID.fromString(envelope.getSourceUuid()));
    assertEquals(Device.PRIMARY_ID, envelope.getSourceDevice());
    assertFalse(updatedPhoneNumberIdentifiersByAccount.containsKey(account));
  }

  @Test
  void changeNumberMismatchedRegistrationId() {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");

    final List<Device> devices = new ArrayList<>();

    for (byte i = 1; i <= 3; i++) {
      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(i);
      when(device.isEnabled()).thenReturn(true);
      when(device.getRegistrationId()).thenReturn((int) i);

      devices.add(device);
      when(account.getDevice(i)).thenReturn(Optional.of(device));
    }

    when(account.getDevices()).thenReturn(devices);

    final byte destinationDeviceId2 = 2;
    final byte destinationDeviceId3 = 3;
    final List<IncomingMessage> messages = List.of(
        new IncomingMessage(1, destinationDeviceId2, 1, "foo"),
        new IncomingMessage(1, destinationDeviceId3, 1, "foo"));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final ECPublicKey pniIdentityKey = pniIdentityKeyPair.getPublicKey();

    final Map<Byte, ECSignedPreKey> preKeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        destinationDeviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair),
        destinationDeviceId3, KeysHelper.signedECPreKey(3, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, destinationDeviceId2, 47,
        destinationDeviceId3, 89);

    assertThrows(StaleDevicesException.class,
        () -> changeNumberManager.changeNumber(account, "+18005559876", new IdentityKey(Curve.generateKeyPair().getPublicKey()), preKeys, null, messages, registrationIds));
  }

  @Test
  void updatePniKeysMismatchedRegistrationId() {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");

    final List<Device> devices = new ArrayList<>();

    for (byte i = 1; i <= 3; i++) {
      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(i);
      when(device.isEnabled()).thenReturn(true);
      when(device.getRegistrationId()).thenReturn((int) i);

      devices.add(device);
      when(account.getDevice(i)).thenReturn(Optional.of(device));
    }

    when(account.getDevices()).thenReturn(devices);

    final byte destinationDeviceId2 = 2;
    final byte destinationDeviceId3 = 3;
    final List<IncomingMessage> messages = List.of(
        new IncomingMessage(1, destinationDeviceId2, 1, "foo"),
        new IncomingMessage(1, destinationDeviceId3, 1, "foo"));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final ECPublicKey pniIdentityKey = pniIdentityKeyPair.getPublicKey();

    final Map<Byte, ECSignedPreKey> preKeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        destinationDeviceId2, KeysHelper.signedECPreKey(2, pniIdentityKeyPair),
        destinationDeviceId3, KeysHelper.signedECPreKey(3, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, destinationDeviceId2, 47,
        destinationDeviceId3, 89);

    assertThrows(StaleDevicesException.class,
        () -> changeNumberManager.updatePniKeys(account, new IdentityKey(Curve.generateKeyPair().getPublicKey()), preKeys, null, messages, registrationIds));
  }

  @Test
  void changeNumberMissingData() {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");

    final List<Device> devices = new ArrayList<>();

    for (byte i = 1; i <= 3; i++) {
      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(i);
      when(device.isEnabled()).thenReturn(true);
      when(device.getRegistrationId()).thenReturn((int) i);

      devices.add(device);
      when(account.getDevice(i)).thenReturn(Optional.of(device));
    }

    when(account.getDevices()).thenReturn(devices);

    final byte destinationDeviceId2 = 2;
    final byte destinationDeviceId3 = 3;
    final List<IncomingMessage> messages = List.of(
        new IncomingMessage(1, destinationDeviceId2, 2, "foo"),
        new IncomingMessage(1, destinationDeviceId3, 3, "foo"));

    final Map<Byte, Integer> registrationIds = Map.of((byte) 1, 17, destinationDeviceId2, 47, destinationDeviceId3, 89);

    assertThrows(IllegalArgumentException.class,
        () -> changeNumberManager.changeNumber(account, "+18005559876", new IdentityKey(Curve.generateKeyPair().getPublicKey()), null, null, messages, registrationIds));
  }
}
