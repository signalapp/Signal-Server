/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Optional;

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

  public record DeviceUpdate(SignedPreKey signedPhoneNumberIdentityPreKey, IncomingMessage message, Integer registrationID) {
  }

  public Account changeNumber(
      @NotNull Account account,
      @NotNull final String number,
      @NotNull final Map<Long, DeviceUpdate> deviceUpdates) throws InterruptedException {

    final Account updatedAccount;
    if (number.equals(account.getNumber())) {
      // This may be a request that got repeated due to poor network conditions or other client error; take no action,
      // but report success since the account is in the desired state
      updatedAccount = account;
    } else {
      updatedAccount = accountsManager.changeNumber(account, number);
    }

    // Whether the account already has this number or not, we reset signed prekeys and resend messages.
    // This makes it so the client can resend a request they didn't get a response for (timeout, etc)
    // to make sure their messages sent and prekeys were updated, even if the first time around the
    // server crashed at/above this point.
    if (deviceUpdates != null && !deviceUpdates.isEmpty()) {
      for (Map.Entry<Long, DeviceUpdate> entry : deviceUpdates.entrySet()) {
        DeviceUpdate deviceUpdate = entry.getValue();
        accountsManager.updateDevice(updatedAccount, entry.getKey(),
            device -> {
              if (deviceUpdate.signedPhoneNumberIdentityPreKey() != null) device.setPhoneNumberIdentitySignedPreKey(deviceUpdate.signedPhoneNumberIdentityPreKey());
              if (deviceUpdate.registrationID() != null) device.setRegistrationId(deviceUpdate.registrationID());
            });
        if (deviceUpdate.message() != null) {
          sendMessageToSelf(updatedAccount, updatedAccount.getDevice(entry.getKey()), deviceUpdate.message());
        }
      }
    }
    return updatedAccount;
  }

  @VisibleForTesting
  void sendMessageToSelf(
      Account sourceAndDestinationAccount, Optional<Device> destinationDevice, IncomingMessage message) {
    Optional<byte[]> contents = MessageController.getMessageContent(message);
    if (!contents.isPresent()) {
      logger.debug("empty message contents sending to self, ignoring");
      return;
    } else if (!destinationDevice.isPresent()) {
      logger.debug("destination device not present");
      return;
    }
    try {
      long serverTimestamp = System.currentTimeMillis();
      Envelope envelope = Envelope.newBuilder()
          .setType(Envelope.Type.forNumber(message.getType()))
          .setTimestamp(serverTimestamp)
          .setServerTimestamp(serverTimestamp)
          .setDestinationUuid(sourceAndDestinationAccount.getUuid().toString())
          .setContent(ByteString.copyFrom(contents.get()))
          .setSource(sourceAndDestinationAccount.getNumber())
          .setSourceUuid(sourceAndDestinationAccount.getUuid().toString())
          .setSourceDevice((int) Device.MASTER_ID)
          .build();
      messageSender.sendMessage(sourceAndDestinationAccount, destinationDevice.get(), envelope, false);
    } catch (NotPushRegisteredException e) {
      logger.debug("Not registered", e);
    }
  }
}
