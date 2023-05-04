/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static java.util.Objects.requireNonNull;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsSelector;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.entities.AuthCheckRequest;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponse;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

@Path("/v1/backup")
@Tag(name = "Secure Value Recovery")
public class SecureBackupController {

  private static final long MAX_AGE_SECONDS = TimeUnit.DAYS.toSeconds(30);

  private final ExternalServiceCredentialsGenerator credentialsGenerator;

  private final AccountsManager accountsManager;

  public static ExternalServiceCredentialsGenerator credentialsGenerator(final SecureBackupServiceConfiguration cfg) {
    return credentialsGenerator(cfg, Clock.systemUTC());
  }

  public static ExternalServiceCredentialsGenerator credentialsGenerator(
      final SecureBackupServiceConfiguration cfg,
      final Clock clock) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.getUserAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .withClock(clock)
        .build();
  }

  public SecureBackupController(
      final ExternalServiceCredentialsGenerator credentialsGenerator,
      final AccountsManager accountsManager) {
    this.credentialsGenerator = requireNonNull(credentialsGenerator);
    this.accountsManager = requireNonNull(accountsManager);
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Generate credentials for SVR",
      description = """
          Generate SVR service credentials. Generated credentials have an expiration time of 30 days 
          (however, the TTL is fully controlled by the server side and may change even for already generated credentials). 
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with generated credentials.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public ExternalServiceCredentials getAuth(final @Auth AuthenticatedAccount auth) {
    return credentialsGenerator.generateForUuid(auth.getAccount().getUuid());
  }

  @Timed
  @POST
  @Path("/auth/check")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.BACKUP_AUTH_CHECK)
  @Operation(
      summary = "Check SVR credentials",
      description = """
          Over time, clients may wind up with multiple sets of KBS authentication credentials in cloud storage. 
          To determine which set is most current and should be used to communicate with SVR to retrieve a master key
          (from which a registration recovery password can be derived), clients should call this endpoint 
          with a list of stored credentials. The response will identify which (if any) set of credentials are appropriate for communicating with SVR.
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with the check results.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "422", description = "Provided list of KBS credentials could not be parsed")
  @ApiResponse(responseCode = "400", description = "`POST` request body is not a valid `JSON`")
  public AuthCheckResponse authCheck(@NotNull @Valid final AuthCheckRequest request) {
    final List<ExternalServiceCredentialsSelector.CredentialInfo> credentials = ExternalServiceCredentialsSelector.check(
        request.passwords(),
        credentialsGenerator,
        MAX_AGE_SECONDS);

    final Predicate<UUID> uuidMatches = accountsManager
        .getByE164(request.number())
        .map(account -> (Predicate<UUID>) account.getUuid()::equals)
        .orElse(candidateUuid -> false);

    return new AuthCheckResponse(credentials.stream().collect(Collectors.toMap(
        ExternalServiceCredentialsSelector.CredentialInfo::token,
        info -> {
          if (!info.valid()) {
            return AuthCheckResponse.Result.INVALID;
          }
          final String username = info.credentials().username();
          // does this credential match the account id for the e164 provided in the request?
          boolean match = UUIDUtil.fromStringSafe(username).filter(uuidMatches).isPresent();
          return match ? AuthCheckResponse.Result.MATCH : AuthCheckResponse.Result.NO_MATCH;
        }
    )));
  }
}
