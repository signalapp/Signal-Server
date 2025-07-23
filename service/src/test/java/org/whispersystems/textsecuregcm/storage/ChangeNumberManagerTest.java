/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
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

      final UUID uuid = account.getIdentifier(IdentityType.ACI);
      final List<Device> devices = account.getDevices();

      final UUID updatedPni = UUID.randomUUID();
      updatedPhoneNumberIdentifiersByAccount.put(account, updatedPni);

      final Account updatedAccount = mock(Account.class);
      when(updatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(uuid);
      when(updatedAccount.getIdentifier(IdentityType.PNI)).thenReturn(updatedPni);
      when(updatedAccount.isIdentifiedBy(any())).thenReturn(false);
      when(updatedAccount.isIdentifiedBy(new AciServiceIdentifier(uuid))).thenReturn(true);
      when(updatedAccount.isIdentifiedBy(new PniServiceIdentifier(updatedPni))).thenReturn(true);
      when(updatedAccount.getNumber()).thenReturn(number);
      when(updatedAccount.getDevices()).thenReturn(devices);
      when(updatedAccount.getDevice(anyByte())).thenReturn(Optional.empty());

      account.getDevices().forEach(device ->
          when(updatedAccount.getDevice(device.getId())).thenReturn(Optional.of(device)));

      return updatedAccount;
    });
  }

  @Test
  void changeNumberSingleDevice() throws Exception {
    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    final Map<Byte, ECSignedPreKey> ecSignedPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(accountIdentifier))).thenReturn(true);

    changeNumberManager.changeNumber(account, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyList(), Collections.emptyMap(), null);
    verify(accountsManager).changeNumber(account, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyMap());
    verify(messageSender, never()).sendMessages(eq(account), any(), any(), any(), any(), any());
  }

  @Test
  void changeNumberLinkedDevices() throws Exception {
    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final UUID aci = UUID.randomUUID();

    final byte primaryDeviceId = Device.PRIMARY_ID;
    final byte linkedDeviceId = primaryDeviceId + 1;

    final int primaryDeviceRegistrationId = 17;
    final int linkedDeviceRegistrationId = primaryDeviceRegistrationId + 1;

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(primaryDeviceId);
    when(primaryDevice.getRegistrationId(IdentityType.ACI)).thenReturn(primaryDeviceRegistrationId);

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn(linkedDeviceId);
    when(linkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(linkedDeviceRegistrationId);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(aci);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(aci))).thenReturn(true);
    when(account.getDevice(anyByte())).thenReturn(Optional.empty());
    when(account.getDevice(primaryDeviceId)).thenReturn(Optional.of(primaryDevice));
    when(account.getDevice(linkedDeviceId)).thenReturn(Optional.of(linkedDevice));
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> ecSignedPreKeys = Map.of(
        primaryDeviceId, KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        linkedDeviceId, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys = Map.of(
        primaryDeviceId, KeysHelper.signedKEMPreKey(3, pniIdentityKeyPair),
        linkedDeviceId, KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    final Map<Byte, Integer> registrationIds = Map.of(
        primaryDeviceId, primaryDeviceRegistrationId,
        linkedDeviceId, linkedDeviceRegistrationId);

    final IncomingMessage incomingMessage =
        new IncomingMessage(1, linkedDeviceId, linkedDeviceRegistrationId, new byte[] { 1 });

    changeNumberManager.changeNumber(account,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        List.of(incomingMessage),
        registrationIds,
        null);

    verify(accountsManager).changeNumber(account,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        registrationIds);

    final MessageProtos.Envelope expectedEnvelope = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(incomingMessage.type()))
        .setClientTimestamp(CLOCK.millis())
        .setServerTimestamp(CLOCK.millis())
        .setDestinationServiceId(new AciServiceIdentifier(aci).toServiceIdentifierString())
        .setContent(ByteString.copyFrom(incomingMessage.content()))
        .setSourceServiceId(new AciServiceIdentifier(aci).toServiceIdentifierString())
        .setSourceDevice(primaryDeviceId)
        .setUpdatedPni(updatedPhoneNumberIdentifiersByAccount.get(account).toString())
        .setUrgent(true)
        .setEphemeral(false)
        .setStory(false)
        .build();

    verify(messageSender).sendMessages(argThat(a -> a.getIdentifier(IdentityType.ACI).equals(aci)),
        eq(new AciServiceIdentifier(aci)),
        eq(Map.of(linkedDeviceId, expectedEnvelope)),
        eq(Map.of(linkedDeviceId, linkedDeviceRegistrationId)),
        eq(Optional.of(primaryDeviceId)),
        any());
  }
}
