/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

public class ReceiptSender {

  private final MessageSender   messageSender;
  private final AccountsManager accountManager;

  private static final Logger logger = LoggerFactory.getLogger(ReceiptSender.class);

  public ReceiptSender(final AccountsManager accountManager, final MessageSender messageSender) {
    this.accountManager = accountManager;
    this.messageSender = messageSender;
  }

  public void sendReceipt(AuthenticatedAccount source, UUID destinationUuid, long messageId) throws NoSuchUserException {
    final Account sourceAccount = source.getAccount();
    if (sourceAccount.getUuid().equals(destinationUuid)) {
      return;
    }

    final Account destinationAccount = accountManager.getByAccountIdentifier(destinationUuid)
        .orElseThrow(() -> new NoSuchUserException(destinationUuid));

    final Envelope.Builder message = Envelope.newBuilder()
        .setServerTimestamp(System.currentTimeMillis())
        .setSource(sourceAccount.getNumber())
        .setSourceUuid(sourceAccount.getUuid().toString())
        .setSourceDevice((int) source.getAuthenticatedDevice().getId())
        .setDestinationUuid(destinationUuid.toString())
        .setTimestamp(messageId)
        .setType(Envelope.Type.SERVER_DELIVERY_RECEIPT);

    if (sourceAccount.getRelay().isPresent()) {
      message.setRelay(sourceAccount.getRelay().get());
    }

    for (final Device destinationDevice : destinationAccount.getDevices()) {
      try {
        messageSender.sendMessage(destinationAccount, destinationDevice, message.build(), false);
      } catch (final NotPushRegisteredException e) {
        logger.info("User no longer push registered for delivery receipt: " + e.getMessage());
      }
    }
  }
}
