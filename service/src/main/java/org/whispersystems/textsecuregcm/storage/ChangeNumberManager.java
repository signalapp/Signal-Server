/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;

public class ChangeNumberManager {

  private static final Logger logger = LoggerFactory.getLogger(ChangeNumberManager.class);
  private final MessageSender messageSender;
  private final AccountsManager accountsManager;
  private final Clock clock;

  public ChangeNumberManager(
      final MessageSender messageSender,
      final AccountsManager accountsManager,
      final Clock clock) {

    this.messageSender = messageSender;
    this.accountsManager = accountsManager;
    this.clock = clock;
  }

  public Account changeNumber(final Account account,
      final String number,
      final IdentityKey pniIdentityKey,
      final Map<Byte, ECSignedPreKey> deviceSignedPreKeys,
      final Map<Byte, KEMSignedPreKey> devicePqLastResortPreKeys,
      final List<IncomingMessage> deviceMessages,
      final Map<Byte, Integer> pniRegistrationIds,
      final String senderUserAgent)
      throws InterruptedException, MismatchedDevicesException, MessageTooLargeException {

    final Account updatedAccount = accountsManager.changeNumber(
        account, number, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, pniRegistrationIds);

    sendDeviceMessages(updatedAccount, deviceMessages, senderUserAgent);

    return updatedAccount;
  }

  private void sendDeviceMessages(final Account account,
      final List<IncomingMessage> deviceMessages,
      final String senderUserAgent) throws MessageTooLargeException, MismatchedDevicesException {

    try {
      final long serverTimestamp = clock.millis();
      final ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(account.getUuid());

      final Map<Byte, Envelope> messagesByDeviceId = deviceMessages.stream()
          .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, message -> Envelope.newBuilder()
              .setType(Envelope.Type.forNumber(message.type()))
              .setClientTimestamp(serverTimestamp)
              .setServerTimestamp(serverTimestamp)
              .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
              .setContent(ByteString.copyFrom(message.content()))
              .setSourceServiceId(serviceIdentifier.toServiceIdentifierString())
              .setSourceDevice(Device.PRIMARY_ID)
              .setUpdatedPni(account.getPhoneNumberIdentifier().toString())
              .setUrgent(true)
              .setEphemeral(false)
              .build()));

      final Map<Byte, Integer> registrationIdsByDeviceId = deviceMessages.stream()
          .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, IncomingMessage::destinationRegistrationId));

      messageSender.sendMessages(account,
          serviceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          Optional.of(Device.PRIMARY_ID),
          senderUserAgent);
    } catch (final RuntimeException e) {
      logger.warn("Changed number but could not send all device messages on {}", account.getUuid(), e);
      throw e;
    }
  }
}
