package org.whispersystems.textsecuregcm.controllers;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.PublicAccount;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Optional;

@Path("/v1/transparency/")
public class TransparentDataController {

  private final AccountsManager     accountsManager;
  private final Map<String, String> transparentDataIndex;

  public TransparentDataController(AccountsManager accountsManager,
                                   Map<String, String> transparentDataIndex)
  {
    this.accountsManager      = accountsManager;
    this.transparentDataIndex = transparentDataIndex;
  }

  @GET
  @Path("/account/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PublicAccount> getAccount(@PathParam("id") String id) {
    String index = transparentDataIndex.get(id);

    if (index != null) {
      return accountsManager.get(index).map(PublicAccount::new);
    }

    return Optional.empty();
  }


}
