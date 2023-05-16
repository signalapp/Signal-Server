/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.StaleDevicesException;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.util.DestinationDeviceValidator;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ChangeNumberManager {
  private static final Logger logger = LoggerFactory.getLogger(AccountController.class);
  private final MessageSender messageSender;
  private final AccountsManager accountsManager;

  public ChangeNumberManager(
      final MessageSender messageSender,
      final AccountsManager accountsManager) {
    this.messageSender = messageSender;
    this.accountsManager = accountsManager;
  }

  public Account changeNumber(final Account account, final String number,
      @Nullable final String pniIdentityKey,
      @Nullable final Map<Long, SignedPreKey> deviceSignedPreKeys,
      @Nullable final Map<Long, SignedPreKey> devicePqLastResortPreKeys,
      @Nullable final List<IncomingMessage> deviceMessages,
      @Nullable final Map<Long, Integer> pniRegistrationIds)
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
      final String pniIdentityKey,
      final Map<Long, SignedPreKey> deviceSignedPreKeys,
      @Nullable final Map<Long, SignedPreKey> devicePqLastResortPreKeys,
      final List<IncomingMessage> deviceMessages,
      final Map<Long, Integer> pniRegistrationIds) throws MismatchedDevicesException, StaleDevicesException {
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
    // Check that all except master ID are in device messages
    DestinationDeviceValidator.validateCompleteDeviceList(
        account,
        deviceMessages.stream().map(IncomingMessage::destinationDeviceId).collect(Collectors.toSet()),
        Set.of(Device.MASTER_ID));

    // check that all sync messages are to the current registration ID for the matching device
    DestinationDeviceValidator.validateRegistrationIds(
        account,
        deviceMessages,
        IncomingMessage::destinationDeviceId,
        IncomingMessage::destinationRegistrationId,
        false);
  }

  private void sendDeviceMessages(final Account account, final List<IncomingMessage> deviceMessages) {
    deviceMessages.forEach(message ->
        sendMessageToSelf(account, account.getDevice(message.destinationDeviceId()), message));
  }

  @VisibleForTesting
  void sendMessageToSelf(
      Account sourceAndDestinationAccount, Optional<Device> destinationDevice, IncomingMessage message) {
    Optional<byte[]> contents = MessageController.getMessageContent(message);
    if (contents.isEmpty()) {
      logger.debug("empty message contents sending to self, ignoring");
      return;
    } else if (destinationDevice.isEmpty()) {
      logger.debug("destination device not present");
      return;
    }
    try {
      long serverTimestamp = System.currentTimeMillis();
      Envelope envelope = Envelope.newBuilder()
          .setType(Envelope.Type.forNumber(message.type()))
          .setTimestamp(serverTimestamp)
          .setServerTimestamp(serverTimestamp)
          .setDestinationUuid(sourceAndDestinationAccount.getUuid().toString())
          .setContent(ByteString.copyFrom(contents.get()))
          .setSourceUuid(sourceAndDestinationAccount.getUuid().toString())
          .setSourceDevice((int) Device.MASTER_ID)
          .setUpdatedPni(sourceAndDestinationAccount.getPhoneNumberIdentifier().toString())
          .setUrgent(true)
          .build();

      messageSender.sendMessage(sourceAndDestinationAccount, destinationDevice.get(), envelope, false);
    } catch (NotPushRegisteredException e) {
      logger.debug("Not registered", e);
    }
  }
}
