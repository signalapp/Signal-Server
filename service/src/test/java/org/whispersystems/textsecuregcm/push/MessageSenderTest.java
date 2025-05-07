/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevices;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.MultiRecipientMismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.tests.util.MultiRecipientMessageHelper;
import org.whispersystems.textsecuregcm.tests.util.TestRecipient;

class MessageSenderTest {

  private MessagesManager messagesManager;
  private PushNotificationManager pushNotificationManager;
  private MessageSender messageSender;
  private ExperimentEnrollmentManager experimentEnrollmentManager;

  @BeforeEach
  void setUp() {
    messagesManager = mock(MessagesManager.class);
    pushNotificationManager = mock(PushNotificationManager.class);
    experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);

    messageSender = new MessageSender(messagesManager, pushNotificationManager, experimentEnrollmentManager);
  }


  @CartesianTest
  void sendMessage(@CartesianTest.Values(booleans = {true, false}) final boolean clientPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean ephemeral,
      @CartesianTest.Values(booleans = {true, false}) final boolean urgent,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasPushToken) throws NotPushRegisteredException {

    final boolean expectPushNotificationAttempt = !clientPresent && !ephemeral;

    final UUID accountIdentifier = UUID.randomUUID();
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(accountIdentifier);
    final byte deviceId = Device.PRIMARY_ID;
    final int registrationId = 17;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final MessageProtos.Envelope message = MessageProtos.Envelope.newBuilder()
        .setEphemeral(ephemeral)
        .setUrgent(urgent)
        .build();

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(serviceIdentifier)).thenReturn(true);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(device.getId()).thenReturn(deviceId);
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);

    if (hasPushToken) {
      when(device.getApnId()).thenReturn("apns-token");
    } else {
      doThrow(NotPushRegisteredException.class)
          .when(pushNotificationManager).sendNewMessageNotification(any(), anyByte(), anyBoolean());
    }

    when(messagesManager.insert(any(), any())).thenReturn(Map.of(deviceId, clientPresent));

    assertDoesNotThrow(() -> messageSender.sendMessages(account,
        serviceIdentifier,
        Map.of(device.getId(), message),
        Map.of(device.getId(), registrationId),
        Optional.empty(),
        null));

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

  @Test
  void sendMessageMismatchedDevices() {
    final UUID accountIdentifier = UUID.randomUUID();
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(accountIdentifier);
    final byte deviceId = Device.PRIMARY_ID;
    final int registrationId = 17;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);
    final MessageProtos.Envelope message = MessageProtos.Envelope.newBuilder().build();

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(serviceIdentifier)).thenReturn(true);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(device.getId()).thenReturn(deviceId);
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
    when(device.getApnId()).thenReturn("apns-token");

    final MismatchedDevicesException mismatchedDevicesException =
        assertThrows(MismatchedDevicesException.class, () -> messageSender.sendMessages(account,
            serviceIdentifier,
            Map.of(device.getId(), message),
            Map.of(device.getId(), registrationId + 1),
            Optional.empty(),
            null));

    assertEquals(new MismatchedDevices(Collections.emptySet(), Collections.emptySet(), Set.of(deviceId)),
        mismatchedDevicesException.getMismatchedDevices());
  }

  @CartesianTest
  void sendMultiRecipientMessage(@CartesianTest.Values(booleans = {true, false}) final boolean clientPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean ephemeral,
      @CartesianTest.Values(booleans = {true, false}) final boolean urgent,
      @CartesianTest.Values(booleans = {true, false}) final boolean hasPushToken)
      throws NotPushRegisteredException, InvalidMessageException, InvalidVersionException {

    final boolean expectPushNotificationAttempt = !clientPresent && !ephemeral;

    final UUID accountIdentifier = UUID.randomUUID();
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(accountIdentifier);
    final byte deviceId = Device.PRIMARY_ID;
    final int registrationId = 17;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(serviceIdentifier)).thenReturn(true);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(device.getId()).thenReturn(deviceId);
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
    when(device.getApnId()).thenReturn("apns-token");

    if (hasPushToken) {
      when(device.getApnId()).thenReturn("apns-token");
    } else {
      doThrow(NotPushRegisteredException.class)
          .when(pushNotificationManager).sendNewMessageNotification(any(), anyByte(), anyBoolean());
    }

    when(messagesManager.insertMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(Map.of(account, Map.of(deviceId, clientPresent))));

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        SealedSenderMultiRecipientMessage.parse(MultiRecipientMessageHelper.generateMultiRecipientMessage(
            List.of(new TestRecipient(serviceIdentifier, deviceId, registrationId, new byte[48]))));

    final SealedSenderMultiRecipientMessage.Recipient recipient =
        multiRecipientMessage.getRecipients().values().iterator().next();

    assertDoesNotThrow(() -> messageSender.sendMultiRecipientMessage(multiRecipientMessage,
            Map.of(recipient, account),
            System.currentTimeMillis(),
            false,
            ephemeral,
            urgent,
            null)
        .join());

    if (expectPushNotificationAttempt) {
      verify(pushNotificationManager).sendNewMessageNotification(account, deviceId, urgent);
    } else {
      verifyNoInteractions(pushNotificationManager);
    }
  }

  @Test
  void sendMultiRecipientMessageMismatchedDevices() throws InvalidMessageException, InvalidVersionException {
    final UUID accountIdentifier = UUID.randomUUID();
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(accountIdentifier);
    final byte deviceId = Device.PRIMARY_ID;
    final int registrationId = 17;

    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(serviceIdentifier)).thenReturn(true);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
    when(device.getId()).thenReturn(deviceId);
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);
    when(device.getApnId()).thenReturn("apns-token");

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        SealedSenderMultiRecipientMessage.parse(MultiRecipientMessageHelper.generateMultiRecipientMessage(
            List.of(new TestRecipient(serviceIdentifier, deviceId, registrationId + 1, new byte[48]))));

    final SealedSenderMultiRecipientMessage.Recipient recipient =
        multiRecipientMessage.getRecipients().values().iterator().next();

    when(messagesManager.insertMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(Map.of(account, Map.of(deviceId, true))));

    final MultiRecipientMismatchedDevicesException mismatchedDevicesException =
        assertThrows(MultiRecipientMismatchedDevicesException.class,
            () -> messageSender.sendMultiRecipientMessage(multiRecipientMessage,
                    Map.of(recipient, account),
                    System.currentTimeMillis(),
                    false,
                    false,
                    true,
                    null)
                .join());

    assertEquals(Map.of(serviceIdentifier, new MismatchedDevices(Collections.emptySet(), Collections.emptySet(), Set.of(deviceId))),
        mismatchedDevicesException.getMismatchedDevicesByServiceIdentifier());
  }

  @Test
  void validateContentLength() {
    assertThrows(MessageTooLargeException.class, () ->
        MessageSender.validateContentLength(MessageSender.MAX_MESSAGE_SIZE + 1, false, false, false, null));

    assertDoesNotThrow(() ->
        MessageSender.validateContentLength(MessageSender.MAX_MESSAGE_SIZE, false, false, false, null));
  }

  @ParameterizedTest
  @MethodSource
  void getMismatchedDevices(final Account account,
      final ServiceIdentifier serviceIdentifier,
      final Map<Byte, Integer> registrationIdsByDeviceId,
      final byte excludedDeviceId,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<MismatchedDevices> expectedMismatchedDevices) {

    assertEquals(expectedMismatchedDevices,
        MessageSender.getMismatchedDevices(account, serviceIdentifier, registrationIdsByDeviceId, excludedDeviceId));
  }

  private static List<Arguments> getMismatchedDevices() {
    final byte primaryDeviceId = Device.PRIMARY_ID;
    final byte linkedDeviceId = primaryDeviceId + 1;
    final byte extraDeviceId = linkedDeviceId + 1;

    final int primaryDeviceAciRegistrationId = 2;
    final int primaryDevicePniRegistrationId = 3;
    final int linkedDeviceAciRegistrationId = 5;
    final int linkedDevicePniRegistrationId = 7;

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(primaryDeviceId);
    when(primaryDevice.getRegistrationId(IdentityType.ACI)).thenReturn(primaryDeviceAciRegistrationId);
    when(primaryDevice.getRegistrationId(IdentityType.PNI)).thenReturn(primaryDevicePniRegistrationId);

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn(linkedDeviceId);
    when(linkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(linkedDeviceAciRegistrationId);
    when(linkedDevice.getRegistrationId(IdentityType.PNI)).thenReturn(linkedDevicePniRegistrationId);

    final Account account = mock(Account.class);
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));
    when(account.getDevice(anyByte())).thenReturn(Optional.empty());
    when(account.getDevice(primaryDeviceId)).thenReturn(Optional.of(primaryDevice));
    when(account.getDevice(linkedDeviceId)).thenReturn(Optional.of(linkedDevice));

    final AciServiceIdentifier aciServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final PniServiceIdentifier pniServiceIdentifier = new PniServiceIdentifier(UUID.randomUUID());

    return List.of(
        Arguments.argumentSet("Complete device list for ACI, no devices excluded",
            account,
            aciServiceIdentifier,
            Map.of(
                primaryDeviceId, primaryDeviceAciRegistrationId,
                linkedDeviceId, linkedDeviceAciRegistrationId
            ),
            MessageSender.NO_EXCLUDED_DEVICE_ID,
            Optional.empty()),

        Arguments.argumentSet("Complete device list for PNI, no devices excluded",
            account,
            pniServiceIdentifier,
            Map.of(
                primaryDeviceId, primaryDevicePniRegistrationId,
                linkedDeviceId, linkedDevicePniRegistrationId
            ),
            MessageSender.NO_EXCLUDED_DEVICE_ID,
            Optional.empty()),

        Arguments.argumentSet("Complete device list, device excluded",
            account,
            aciServiceIdentifier,
            Map.of(
                linkedDeviceId, linkedDeviceAciRegistrationId
            ),
            primaryDeviceId,
            Optional.empty()),

        Arguments.argumentSet("Mismatched devices",
            account,
            aciServiceIdentifier,
            Map.of(
                linkedDeviceId, linkedDeviceAciRegistrationId + 1,
                extraDeviceId, 17
            ),
            MessageSender.NO_EXCLUDED_DEVICE_ID,
            Optional.of(new MismatchedDevices(Set.of(primaryDeviceId), Set.of(extraDeviceId), Set.of(linkedDeviceId))))
    );
  }

  @Test
  void sendMessageEmptyMessageList() {
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    final Account account = mock(Account.class);
    when(account.getDevices()).thenReturn(List.of(device));
    when(account.isIdentifiedBy(serviceIdentifier)).thenReturn(true);

    assertThrows(MismatchedDevicesException.class, () -> messageSender.sendMessages(account,
        serviceIdentifier,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.empty(),
        null));

    assertDoesNotThrow(() -> messageSender.sendMessages(account,
        serviceIdentifier,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.of(Device.PRIMARY_ID),
        null));
  }

  @Test
  void sendSyncMessageMismatchedAddressing() {
    final UUID accountIdentifier = UUID.randomUUID();
    final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(accountIdentifier);
    final byte deviceId = Device.PRIMARY_ID;

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(accountIdentifier);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(serviceIdentifier)).thenReturn(true);

    final Account nonSyncDestination = mock(Account.class);
    when(nonSyncDestination.isIdentifiedBy(any())).thenReturn(true);

    assertThrows(IllegalArgumentException.class, () -> messageSender.sendMessages(nonSyncDestination,
            new AciServiceIdentifier(UUID.randomUUID()),
            Map.of(deviceId, MessageProtos.Envelope.newBuilder().build()),
            Map.of(deviceId, 17),
            Optional.of(deviceId),
            null),
        "Should throw an IllegalArgumentException for inter-account messages with a sync message device ID");

    assertThrows(IllegalArgumentException.class, () -> messageSender.sendMessages(account,
        serviceIdentifier,
        Map.of(deviceId, MessageProtos.Envelope.newBuilder()
            .setSourceServiceId(serviceIdentifier.toServiceIdentifierString())
            .setSourceDevice(deviceId)
            .build()),
        Map.of(deviceId, 17),
        Optional.empty(),
        null),
        "Should throw an IllegalArgumentException for self-addressed messages without a sync message device ID");
  }
}
