/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsSelector;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery3Configuration;
import org.whispersystems.textsecuregcm.entities.AuthCheckRequest;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponseV3;
import org.whispersystems.textsecuregcm.entities.SetShareSetRequest;
import org.whispersystems.textsecuregcm.entities.Svr3Credentials;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Optionals;
import org.whispersystems.websocket.auth.Mutable;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v3/backup")
@Tag(name = "Secure Value Recovery")
public class SecureValueRecovery3Controller {

  private static final long MAX_AGE_SECONDS = TimeUnit.DAYS.toSeconds(30);

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureValueRecovery3Configuration cfg) {
    return credentialsGenerator(cfg, Clock.systemUTC());
  }

  @VisibleForTesting
  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureValueRecovery3Configuration cfg,
      final Clock clock) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret().value())
        .prependUsername(false)
        .withDerivedUsernameTruncateLength(16)
        .withClock(clock)
        .build();
  }

  private final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator;
  private final AccountsManager accountsManager;

  public SecureValueRecovery3Controller(final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator,
      final AccountsManager accountsManager) {
    this.backupServiceCredentialGenerator = backupServiceCredentialGenerator;
    this.accountsManager = accountsManager;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for SVR3",
      description = """
          Generate SVR3 service credentials. Generated credentials have an expiration time of 30 days
          (however, the TTL is fully controlled by the server side and may change even for already generated credentials). 
                    
          If a share-set has been previously set via /v3/backups/share-set, it will be included in the response
          """)
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials and share-set", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public Svr3Credentials getAuth(@ReadOnly @Auth final AuthenticatedDevice auth) {
    final ExternalServiceCredentials creds = backupServiceCredentialGenerator.generateFor(
        auth.getAccount().getUuid().toString());
    return new Svr3Credentials(creds.username(), creds.password(), auth.getAccount().getSvr3ShareSet());
  }

  @PUT
  @Path("/share-set")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Set a share-set for the account",
      description = """
          Add a share-set to the account that can later be retrieved at v3/backups/auth or during registration. After
          storing a value with SVR3, clients must store the returned share-set so the value can be restored later.
          """)
  @ApiResponse(responseCode = "204", description = "Successfully set share-set")
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public void setShareSet(
      @Mutable @Auth final AuthenticatedDevice auth,
      @NotNull @Valid final SetShareSetRequest request) {
    accountsManager.update(auth.getAccount(), account -> account.setSvr3ShareSet(request.shareSet()));
  }

  @POST
  @Path("/auth/check")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.BACKUP_AUTH_CHECK)
  @Operation(
      summary = "Check SVR3 credentials",
      description = """
          Over time, clients may wind up with multiple sets of SVR3 authentication credentials in cloud storage.
          To determine which set is most current and should be used to communicate with SVR3 to retrieve a master key
          (from which a registration recovery password can be derived), clients should call this endpoint
          with a list of stored credentials. The response will identify which (if any) set of credentials are
          appropriate for communicating with SVR3.
          """)
  @ApiResponse(responseCode = "200", description = "`JSON` with the check results.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "422", description = "Provided list of SVR3 credentials could not be parsed")
  @ApiResponse(responseCode = "400", description = "`POST` request body is not a valid `JSON`")
  public AuthCheckResponseV3 authCheck(@NotNull @Valid final AuthCheckRequest request) {
    final List<ExternalServiceCredentialsSelector.CredentialInfo> credentials = ExternalServiceCredentialsSelector.check(
        request.tokens(),
        backupServiceCredentialGenerator,
        MAX_AGE_SECONDS);

    final Optional<Account> account = accountsManager.getByE164(request.number());

    // the username associated with the provided number
    final Optional<String> matchingUsername = account
        .map(Account::getUuid)
        .map(backupServiceCredentialGenerator::generateForUuid)
        .map(ExternalServiceCredentials::username);

    return new AuthCheckResponseV3(credentials.stream().collect(Collectors.toMap(
        ExternalServiceCredentialsSelector.CredentialInfo::token,
        info -> {
          if (!info.valid()) {
            // This isn't a valid credential (could be for a different SVR service, expired, etc)
            return AuthCheckResponseV3.Result.invalid();
          }
          final String credUsername = info.credentials().username();

          return Optionals
              // If the account exists, and the account's username matches this credential's username, return a match
              .zipWith(account, matchingUsername.filter(credUsername::equals), (a, ignored) ->
                  AuthCheckResponseV3.Result.match(a.getSvr3ShareSet()))
              // Otherwise, return no-match
              .orElseGet(AuthCheckResponseV3.Result::noMatch);
        }
    )));
  }
}
