/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
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

    final long serverTimestamp = clock.millis();
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(account.getIdentifier(IdentityType.ACI));

    // Note that these for-validation envelopes do NOT have the "updated PNI" field set, and we'll need to populate that
    // after actually changing the account's number.
    final Map<Byte, Envelope> messagesByDeviceId = deviceMessages.stream()
        .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, message -> message.toEnvelope(serviceIdentifier,
            serviceIdentifier,
            Device.PRIMARY_ID,
            serverTimestamp,
            false,
            false,
            true,
            null,
            clock)));

    final Map<Byte, Integer> registrationIdsByDeviceId = deviceMessages.stream()
        .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, IncomingMessage::destinationRegistrationId));

    // Make sure we can plausibly deliver the messages to other devices on the account before making any changes to the
    // account itself
    if (!messagesByDeviceId.isEmpty()) {
      MessageSender.validateIndividualMessageBundle(account,
          serviceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          Optional.of(Device.PRIMARY_ID),
          senderUserAgent);
    }

    final Account updatedAccount = accountsManager.changeNumber(
        account, number, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, pniRegistrationIds);

    try {
      // Now that we've actually updated the account, populate the "updated PNI" field on all envelopes
      final String updatedPniString = updatedAccount.getIdentifier(IdentityType.PNI).toString();

      messagesByDeviceId.replaceAll((deviceId, envelope) ->
          envelope.toBuilder().setUpdatedPni(updatedPniString).build());

      messageSender.sendMessages(updatedAccount,
          serviceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          Optional.of(Device.PRIMARY_ID),
          senderUserAgent);
    } catch (final RuntimeException e) {
      logger.warn("Changed number but could not send all device messages for {}", account.getIdentifier(IdentityType.ACI), e);
      throw e;
    }

    return updatedAccount;
  }
}
