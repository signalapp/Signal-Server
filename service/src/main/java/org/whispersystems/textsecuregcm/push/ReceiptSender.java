/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.Optional;

public class ReceiptSender {

  private final MessageSender   messageSender;
  private final AccountsManager accountManager;

  private static final Logger logger = LoggerFactory.getLogger(ReceiptSender.class);

  public ReceiptSender(AccountsManager accountManager,
                       MessageSender   messageSender)
  {
    this.accountManager = accountManager;
    this.messageSender  = messageSender;
  }

  public void sendReceipt(Account source, String destination, long messageId)
      throws NoSuchUserException
  {
    if (source.getNumber().equals(destination)) {
      return;
    }

    Account          destinationAccount = getDestinationAccount(destination);
    Envelope.Builder message            = Envelope.newBuilder()
                                                  .setSource(source.getNumber())
                                                  .setSourceUuid(source.getUuid().toString())
                                                  .setSourceDevice((int) source.getAuthenticatedDevice().get().getId())
                                                  .setTimestamp(messageId)
                                                  .setType(Envelope.Type.RECEIPT);

    if (source.getRelay().isPresent()) {
      message.setRelay(source.getRelay().get());
    }

    for (final Device destinationDevice : destinationAccount.getDevices()) {
      try {
        messageSender.sendMessage(destinationAccount, destinationDevice, message.build(), false);
      } catch (NotPushRegisteredException e) {
        logger.info("User no longer push registered for delivery receipt: " + e.getMessage());
      }
    }
  }

  private Account getDestinationAccount(String destination)
      throws NoSuchUserException
  {
    Optional<Account> account = accountManager.get(destination);

    if (!account.isPresent()) {
      throw new NoSuchUserException(destination);
    }

    return account.get();
  }

}
