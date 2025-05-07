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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.TestClock;

public class ChangeNumberManagerTest {
  private AccountsManager accountsManager;
  private MessageSender messageSender;
  private ChangeNumberManager changeNumberManager;

  private Map<Account, UUID> updatedPhoneNumberIdentifiersByAccount;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  @BeforeEach
  void setUp() throws Exception {
    accountsManager = mock(AccountsManager.class);
    messageSender = mock(MessageSender.class);
    changeNumberManager = new ChangeNumberManager(messageSender, accountsManager, CLOCK);

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
    changeNumberManager.changeNumber(account, "+18025551234", null, null, null, null, null, null);
    verify(accountsManager).changeNumber(account, "+18025551234", null, null, null, null);
    verify(accountsManager, never()).updateDevice(any(), anyByte(), any());
    verify(messageSender, never()).sendMessages(eq(account), any(), any(), any(), any(), any());
  }

  @Test
  void changeNumberSetPrimaryDevicePrekey() throws Exception {
    Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(Curve.generateKeyPair().getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    changeNumberManager.changeNumber(account, "+18025551234", pniIdentityKey, prekeys, null, Collections.emptyList(), Collections.emptyMap(), null);
    verify(accountsManager).changeNumber(account, "+18025551234", pniIdentityKey, prekeys, null, Collections.emptyMap());
    verify(messageSender, never()).sendMessages(eq(account), any(), any(), any(), any(), any());
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

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(primaryDevice.getRegistrationId(IdentityType.ACI)).thenReturn(7);

    final Device linkedDevice = mock(Device.class);
    final byte linkedDeviceId = Device.PRIMARY_ID + 1;
    final int linkedDeviceRegistrationId = 17;
    when(linkedDevice.getId()).thenReturn(linkedDeviceId);
    when(linkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(linkedDeviceRegistrationId);

    when(account.getDevice(anyByte())).thenReturn(Optional.empty());
    when(account.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(primaryDevice));
    when(account.getDevice(linkedDeviceId)).thenReturn(Optional.of(linkedDevice));
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> prekeys = Map.of(Device.PRIMARY_ID,
        KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        linkedDeviceId, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
    final Map<Byte, Integer> registrationIds = Map.of(Device.PRIMARY_ID, 17, linkedDeviceId, 19);

    final IncomingMessage msg = mock(IncomingMessage.class);
    when(msg.type()).thenReturn(1);
    when(msg.destinationDeviceId()).thenReturn(linkedDeviceId);
    when(msg.destinationRegistrationId()).thenReturn(linkedDeviceRegistrationId);
    when(msg.content()).thenReturn(new byte[]{1});

    changeNumberManager.changeNumber(account, changedE164, pniIdentityKey, prekeys, null, List.of(msg), registrationIds, null);

    verify(accountsManager).changeNumber(account, changedE164, pniIdentityKey, prekeys, null, registrationIds);

    final MessageProtos.Envelope expectedEnvelope = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(msg.type()))
        .setClientTimestamp(CLOCK.millis())
        .setServerTimestamp(CLOCK.millis())
        .setDestinationServiceId(new AciServiceIdentifier(aci).toServiceIdentifierString())
        .setContent(ByteString.copyFrom(msg.content()))
        .setSourceServiceId(new AciServiceIdentifier(aci).toServiceIdentifierString())
        .setSourceDevice(Device.PRIMARY_ID)
        .setUpdatedPni(updatedPhoneNumberIdentifiersByAccount.get(account).toString())
        .setUrgent(true)
        .setEphemeral(false)
        .build();

    verify(messageSender).sendMessages(argThat(a -> a.getUuid().equals(aci)),
        eq(new AciServiceIdentifier(aci)),
        eq(Map.of(linkedDeviceId, expectedEnvelope)),
        eq(Map.of(linkedDeviceId, linkedDeviceRegistrationId)),
        eq(Optional.of(Device.PRIMARY_ID)),
        any());
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
    when(msg.content()).thenReturn(new byte[]{1});

    changeNumberManager.changeNumber(account, changedE164, pniIdentityKey, prekeys, pqPrekeys, List.of(msg), registrationIds, null);

    verify(accountsManager).changeNumber(account, changedE164, pniIdentityKey, prekeys, pqPrekeys, registrationIds);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, MessageProtos.Envelope>> envelopeCaptor =
        ArgumentCaptor.forClass(Map.class);

    verify(messageSender).sendMessages(any(), any(), envelopeCaptor.capture(), any(), any(), any());

    assertEquals(1, envelopeCaptor.getValue().size());
    assertEquals(Set.of(deviceId2), envelopeCaptor.getValue().keySet());

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue().get(deviceId2);

    assertEquals(aci, UUID.fromString(envelope.getDestinationServiceId()));
    assertEquals(aci, UUID.fromString(envelope.getSourceServiceId()));
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
    when(msg.content()).thenReturn(new byte[]{1});

    changeNumberManager.changeNumber(account, originalE164, pniIdentityKey, prekeys, pqPrekeys, List.of(msg), registrationIds, null);

    verify(accountsManager).updatePniKeys(account, pniIdentityKey, prekeys, pqPrekeys, registrationIds);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, MessageProtos.Envelope>> envelopeCaptor =
        ArgumentCaptor.forClass(Map.class);

    verify(messageSender).sendMessages(any(), any(), envelopeCaptor.capture(), any(), any(), any());

    assertEquals(1, envelopeCaptor.getValue().size());
    assertEquals(Set.of(deviceId2), envelopeCaptor.getValue().keySet());

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue().get(deviceId2);

    assertEquals(aci, UUID.fromString(envelope.getDestinationServiceId()));
    assertEquals(aci, UUID.fromString(envelope.getSourceServiceId()));
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
    when(msg.content()).thenReturn(new byte[]{1});

    changeNumberManager.updatePniKeys(account, pniIdentityKey, prekeys, null, List.of(msg), registrationIds, null);

    verify(accountsManager).updatePniKeys(account, pniIdentityKey, prekeys, null, registrationIds);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, MessageProtos.Envelope>> envelopeCaptor =
        ArgumentCaptor.forClass(Map.class);

    verify(messageSender).sendMessages(any(), any(), envelopeCaptor.capture(), any(), any(), any());

    assertEquals(1, envelopeCaptor.getValue().size());
    assertEquals(Set.of(deviceId2), envelopeCaptor.getValue().keySet());

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue().get(deviceId2);

    assertEquals(aci, UUID.fromString(envelope.getDestinationServiceId()));
    assertEquals(aci, UUID.fromString(envelope.getSourceServiceId()));
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
    when(msg.content()).thenReturn(new byte[]{1});

    changeNumberManager.updatePniKeys(account, pniIdentityKey, prekeys, pqPrekeys, List.of(msg), registrationIds, null);

    verify(accountsManager).updatePniKeys(account, pniIdentityKey, prekeys, pqPrekeys, registrationIds);

    @SuppressWarnings("unchecked") final ArgumentCaptor<Map<Byte, MessageProtos.Envelope>> envelopeCaptor =
        ArgumentCaptor.forClass(Map.class);

    verify(messageSender).sendMessages(any(), any(), envelopeCaptor.capture(), any(), any(), any());

    assertEquals(1, envelopeCaptor.getValue().size());
    assertEquals(Set.of(deviceId2), envelopeCaptor.getValue().keySet());

    final MessageProtos.Envelope envelope = envelopeCaptor.getValue().get(deviceId2);

    assertEquals(aci, UUID.fromString(envelope.getDestinationServiceId()));
    assertEquals(aci, UUID.fromString(envelope.getSourceServiceId()));
    assertEquals(Device.PRIMARY_ID, envelope.getSourceDevice());
    assertFalse(updatedPhoneNumberIdentifiersByAccount.containsKey(account));
  }

  @Test
  void changeNumberMissingData() {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");

    final List<Device> devices = new ArrayList<>();

    for (byte i = 1; i <= 3; i++) {
      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(i);
      when(device.getRegistrationId(IdentityType.ACI)).thenReturn((int) i);

      devices.add(device);
      when(account.getDevice(i)).thenReturn(Optional.of(device));
    }

    when(account.getDevices()).thenReturn(devices);

    final byte destinationDeviceId2 = 2;
    final byte destinationDeviceId3 = 3;
    final List<IncomingMessage> messages = List.of(
        new IncomingMessage(1, destinationDeviceId2, 2, "foo".getBytes(StandardCharsets.UTF_8)),
        new IncomingMessage(1, destinationDeviceId3, 3, "foo".getBytes(StandardCharsets.UTF_8)));

    final Map<Byte, Integer> registrationIds = Map.of((byte) 1, 17, destinationDeviceId2, 47, destinationDeviceId3, 89);

    assertThrows(IllegalArgumentException.class,
        () -> changeNumberManager.changeNumber(account, "+18005559876", new IdentityKey(Curve.generateKeyPair().getPublicKey()), null, null, messages, registrationIds, null));
  }
}
