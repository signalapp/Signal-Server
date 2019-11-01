package org.whispersystems.textsecuregcm.push;

import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.util.Optional;
import java.util.Set;

public class ReceiptSender {

  private final PushSender      pushSender;
  private final AccountsManager accountManager;

  public ReceiptSender(AccountsManager accountManager,
                       PushSender      pushSender)
  {
    this.accountManager = accountManager;
    this.pushSender     = pushSender;
  }

  public void sendReceipt(Account source, String destination, long messageId)
      throws NoSuchUserException, NotPushRegisteredException
  {
    if (source.getNumber().equals(destination)) {
      return;
    }

    Account          destinationAccount = getDestinationAccount(destination);
    Set<Device>      destinationDevices = destinationAccount.getDevices();
    Envelope.Builder message            = Envelope.newBuilder()
                                                  .setSource(source.getNumber())
                                                  .setSourceUuid(source.getUuid().toString())
                                                  .setSourceDevice((int) source.getAuthenticatedDevice().get().getId())
                                                  .setTimestamp(messageId)
                                                  .setType(Envelope.Type.RECEIPT);

    if (source.getRelay().isPresent()) {
      message.setRelay(source.getRelay().get());
    }

    for (Device destinationDevice : destinationDevices) {
      pushSender.sendMessage(destinationAccount, destinationDevice, message.build(), false);
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
