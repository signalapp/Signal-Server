package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.TransientPushFailureException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Set;

import io.dropwizard.auth.Auth;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.OutgoingMessageSignal;

@Path("/v1/receipt")
public class ReceiptController {

  private final AccountsManager        accountManager;
  private final PushSender             pushSender;
  private final FederatedClientManager federatedClientManager;

  public ReceiptController(AccountsManager accountManager,
                           FederatedClientManager federatedClientManager,
                           PushSender pushSender)
  {
    this.accountManager         = accountManager;
    this.federatedClientManager = federatedClientManager;
    this.pushSender             = pushSender;
  }

  @Timed
  @PUT
  @Path("/{destination}/{messageId}")
  public void sendDeliveryReceipt(@Auth                     Account source,
                                  @PathParam("destination") String destination,
                                  @PathParam("messageId")   long messageId,
                                  @QueryParam("relay")      Optional<String> relay)
      throws IOException
  {
    try {
      if (relay.isPresent() && !relay.get().isEmpty()) {
        sendRelayedReceipt(source, destination, messageId, relay.get());
      } else {
        sendDirectReceipt(source, destination, messageId);
      }
    } catch (NoSuchUserException | NotPushRegisteredException e) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    } catch (TransientPushFailureException e) {
      throw new IOException(e);
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
    Account     destinationAccount = getDestinationAccount(destination);
    Set<Device> destinationDevices = destinationAccount.getDevices();

    OutgoingMessageSignal.Builder message =
        OutgoingMessageSignal.newBuilder()
                             .setSource(source.getNumber())
                             .setSourceDevice((int) source.getAuthenticatedDevice().get().getId())
                             .setTimestamp(messageId)
                             .setType(OutgoingMessageSignal.Type.RECEIPT_VALUE);

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
