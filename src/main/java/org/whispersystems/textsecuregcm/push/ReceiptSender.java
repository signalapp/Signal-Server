package org.whispersystems.textsecuregcm.push;

import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.io.IOException;
import java.util.Set;

public class ReceiptSender {

  private final PushSender             pushSender;
  private final FederatedClientManager federatedClientManager;
  private final AccountsManager        accountManager;

  public ReceiptSender(AccountsManager        accountManager,
                       PushSender             pushSender,
                       FederatedClientManager federatedClientManager)
  {
    this.federatedClientManager = federatedClientManager;
    this.accountManager         = accountManager;
    this.pushSender             = pushSender;
  }

  public void sendReceipt(Account source, String destination,
                          long messageId, Optional<String> relay)
      throws IOException, NoSuchUserException,
             NotPushRegisteredException, TransientPushFailureException
  {
    if (relay.isPresent() && !relay.get().isEmpty()) {
      sendRelayedReceipt(source, destination, messageId, relay.get());
    } else {
      sendDirectReceipt(source, destination, messageId);
    }
  }

  private void sendRelayedReceipt(Account source, String destination, long messageId, String relay)
      throws NoSuchUserException, IOException
  {
    try {
      federatedClientManager.getClient(relay)
                            .sendDeliveryReceipt(source.getNumber(),
                                                 source.getAuthenticatedDevice().get().getId(),
                                                 destination, messageId);
    } catch (NoSuchPeerException e) {
      throw new NoSuchUserException(e);
    }
  }

  private void sendDirectReceipt(Account source, String destination, long messageId)
      throws NotPushRegisteredException, TransientPushFailureException, NoSuchUserException
  {
    Account          destinationAccount = getDestinationAccount(destination);
    Set<Device>      destinationDevices = destinationAccount.getDevices();
    Envelope.Builder message            = Envelope.newBuilder()
                                                  .setSource(source.getNumber())
                                                  .setSourceDevice((int) source.getAuthenticatedDevice().get().getId())
                                                  .setTimestamp(messageId)
                                                  .setType(Envelope.Type.RECEIPT);

    if (source.getRelay().isPresent()) {
      message.setRelay(source.getRelay().get());
    }

    for (Device destinationDevice : destinationDevices) {
      pushSender.sendMessage(destinationAccount, destinationDevice, message.build());
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
