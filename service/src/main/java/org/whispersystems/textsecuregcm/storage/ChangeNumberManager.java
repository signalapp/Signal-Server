/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
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
      @Nullable final List<IncomingMessage> deviceMessages,
      @Nullable final Map<Long, Integer> pniRegistrationIds)
      throws InterruptedException, MismatchedDevicesException, StaleDevicesException {

    if (ObjectUtils.allNotNull(pniIdentityKey, deviceSignedPreKeys, deviceMessages, pniRegistrationIds)) {
      assert pniIdentityKey != null;
      assert deviceSignedPreKeys != null;
      assert deviceMessages != null;
      assert pniRegistrationIds != null;

      // Check that all except master ID are in device messages
      DestinationDeviceValidator.validateCompleteDeviceList(
          account,
          deviceMessages.stream().map(IncomingMessage::destinationDeviceId).collect(Collectors.toSet()),
          Set.of(Device.MASTER_ID));

      DestinationDeviceValidator.validateRegistrationIds(
          account,
          deviceMessages,
          IncomingMessage::destinationDeviceId,
          IncomingMessage::destinationRegistrationId,
          false);
    } else if (!ObjectUtils.allNull(pniIdentityKey, deviceSignedPreKeys, deviceMessages, pniRegistrationIds)) {
      throw new IllegalArgumentException("PNI identity key, signed pre-keys, device messages, and registration IDs must be all null or all non-null");
    }

    final Account updatedAccount;

    if (number.equals(account.getNumber())) {
      // This may be a request that got repeated due to poor network conditions or other client error; take no action,
      // but report success since the account is in the desired state
      updatedAccount = account;
    } else {
      updatedAccount = accountsManager.changeNumber(account, number, pniIdentityKey, deviceSignedPreKeys, pniRegistrationIds);
    }

    // Whether the account already has this number or not, we resend messages. This makes it so the client can resend a
    // request they didn't get a response for (timeout, etc) to make sure their messages sent even if the first time
    // around the server crashed at/above this point.
    if (deviceMessages != null) {
      deviceMessages.forEach(message ->
          sendMessageToSelf(updatedAccount, updatedAccount.getDevice(message.destinationDeviceId()), message));
    }

    return updatedAccount;
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
