package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.dropwizard.auth.Auth;

@Path("/v1/profile")
public class ProfileController {

  private final RateLimiters    rateLimiters;
  private final AccountsManager accountsManager;

  public ProfileController(RateLimiters rateLimiters, AccountsManager accountsManager) {
    this.rateLimiters    = rateLimiters;
    this.accountsManager = accountsManager;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{number}")
  public Profile getProfile(@Auth Account account, @PathParam("number") String number)
      throws RateLimitExceededException
  {
    rateLimiters.getProfileLimiter().validate(account.getNumber());

    Optional<Account> accountProfile = accountsManager.get(number);

    if (!accountProfile.isPresent()) {
      throw new WebApplicationException(Response.status(404).build());
    }

    return new Profile(accountProfile.get().getIdentityKey());
  }


}
