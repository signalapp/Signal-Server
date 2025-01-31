/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.util.DestinationDeviceValidator;

public class ChangeNumberManager {

  private static final Logger logger = LoggerFactory.getLogger(ChangeNumberManager.class);
  private final MessageSender messageSender;
  private final AccountsManager accountsManager;

  public ChangeNumberManager(
      final MessageSender messageSender,
      final AccountsManager accountsManager) {
    this.messageSender = messageSender;
    this.accountsManager = accountsManager;
  }

  public Account changeNumber(final Account account, final String number,
      @Nullable final IdentityKey pniIdentityKey,
      @Nullable final Map<Byte, ECSignedPreKey> deviceSignedPreKeys,
      @Nullable final Map<Byte, KEMSignedPreKey> devicePqLastResortPreKeys,
      @Nullable final List<IncomingMessage> deviceMessages,
      @Nullable final Map<Byte, Integer> pniRegistrationIds)
      throws InterruptedException, MismatchedDevicesException, StaleDevicesException {

    if (ObjectUtils.allNotNull(pniIdentityKey, deviceSignedPreKeys, deviceMessages, pniRegistrationIds)) {
      // AccountsManager validates the device set on deviceSignedPreKeys and pniRegistrationIds
      validateDeviceMessages(account, deviceMessages);
    } else if (!ObjectUtils.allNull(pniIdentityKey, deviceSignedPreKeys, deviceMessages, pniRegistrationIds)) {
      throw new IllegalArgumentException("PNI identity key, signed pre-keys, device messages, and registration IDs must be all null or all non-null");
    }

    if (number.equals(account.getNumber())) {
      // The client has gotten confused/desynchronized with us about their own phone number, most likely due to losing
      // our OK response to an immediately preceding change-number request, and are sending a change they don't realize
      // is a no-op change.
      //
      // We don't need to actually do a number-change operation in our DB, but we *do* need to accept their new key
      // material and distribute the sync messages, to be sure all clients agree with us and each other about what their
      // keys are. Pretend this change-number request was actually a PNI key distribution request.
      if (pniIdentityKey == null) {
        return account;
      }
      return updatePniKeys(account, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, deviceMessages, pniRegistrationIds);
    }

    final Account updatedAccount = accountsManager.changeNumber(
        account, number, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, pniRegistrationIds);

    if (deviceMessages != null) {
      sendDeviceMessages(updatedAccount, deviceMessages);
    }

    return updatedAccount;
  }

  public Account updatePniKeys(final Account account,
      final IdentityKey pniIdentityKey,
      final Map<Byte, ECSignedPreKey> deviceSignedPreKeys,
      @Nullable final Map<Byte, KEMSignedPreKey> devicePqLastResortPreKeys,
      final List<IncomingMessage> deviceMessages,
      final Map<Byte, Integer> pniRegistrationIds) throws MismatchedDevicesException, StaleDevicesException {
    validateDeviceMessages(account, deviceMessages);

    // Don't try to be smart about ignoring unnecessary retries. If we make literally no change we will skip the ddb
    // write anyway. Linked devices can handle some wasted extra key rotations.
    final Account updatedAccount = accountsManager.updatePniKeys(
        account, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, pniRegistrationIds);

    sendDeviceMessages(updatedAccount, deviceMessages);
    return updatedAccount;
  }

  private void validateDeviceMessages(final Account account,
      final List<IncomingMessage> deviceMessages) throws MismatchedDevicesException, StaleDevicesException {
    // Check that all except primary ID are in device messages
    DestinationDeviceValidator.validateCompleteDeviceList(
        account,
        deviceMessages.stream().map(IncomingMessage::destinationDeviceId).collect(Collectors.toSet()),
        Set.of(Device.PRIMARY_ID));

    // check that all sync messages are to the current registration ID for the matching device
    DestinationDeviceValidator.validateRegistrationIds(
        account,
        deviceMessages,
        IncomingMessage::destinationDeviceId,
        IncomingMessage::destinationRegistrationId,
        false);
  }

  private void sendDeviceMessages(final Account account, final List<IncomingMessage> deviceMessages) {
    try {
      final long serverTimestamp = System.currentTimeMillis();

      messageSender.sendMessages(account, deviceMessages.stream()
          .filter(message -> getMessageContent(message).isPresent())
          .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, message -> Envelope.newBuilder()
              .setType(Envelope.Type.forNumber(message.type()))
              .setClientTimestamp(serverTimestamp)
              .setServerTimestamp(serverTimestamp)
              .setDestinationServiceId(new AciServiceIdentifier(account.getUuid()).toServiceIdentifierString())
              .setContent(ByteString.copyFrom(getMessageContent(message).orElseThrow()))
              .setSourceServiceId(new AciServiceIdentifier(account.getUuid()).toServiceIdentifierString())
              .setSourceDevice(Device.PRIMARY_ID)
              .setUpdatedPni(account.getPhoneNumberIdentifier().toString())
              .setUrgent(true)
              .setEphemeral(false)
              .build())));
    } catch (final RuntimeException e) {
      logger.warn("Changed number but could not send all device messages on {}", account.getUuid(), e);
      throw e;
    }
  }

  private static Optional<byte[]> getMessageContent(final IncomingMessage message) {
    if (StringUtils.isEmpty(message.content())) {
      logger.warn("Message has no content");
      return Optional.empty();
    }

    try {
      return Optional.of(Base64.getDecoder().decode(message.content()));
    } catch (final IllegalArgumentException e) {
      logger.warn("Failed to parse message content", e);
      return Optional.empty();
    }
  }
}
