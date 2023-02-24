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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.tuple.Pair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
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
          To determine which set is most current and should be used to communicate with SVR to retrieve a master password 
          (from which a registration recovery password can be derived), clients should call this endpoint 
          with a list of stored credentials. The response will identify which (if any) set of credentials are appropriate for communicating with SVR.
          """
  )
  @ApiResponse(responseCode = "200", description = "`JSON` with the check results.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "422", description = "Provided list of KBS credentials could not be parsed")
  @ApiResponse(responseCode = "400", description = "`POST` request body is not a valid `JSON`")
  public AuthCheckResponse authCheck(@NotNull @Valid final AuthCheckRequest request) {
    final Map<String, AuthCheckResponse.Result> results = new HashMap<>();
    final Map<String, Pair<UUID, Long>> tokenToUuid = new HashMap<>();
    final Map<UUID, Long> uuidToLatestTimestamp = new HashMap<>();

    // first pass -- filter out all tokens that contain invalid credentials
    // (this could be either legit but expired or illegitimate for any reason)
    request.passwords().forEach(token -> {
      // each token is supposed to be in a "${username}:${password}" form,
      // (note that password part may also contain ':' characters)
      final String[] parts = token.split(":", 2);
      if (parts.length != 2) {
        results.put(token, AuthCheckResponse.Result.INVALID);
        return;
      }
      final ExternalServiceCredentials credentials = new ExternalServiceCredentials(parts[0], parts[1]);
      final Optional<Long> maybeTimestamp = credentialsGenerator.validateAndGetTimestamp(credentials, MAX_AGE_SECONDS);
      final Optional<UUID> maybeUuid = UUIDUtil.fromStringSafe(credentials.username());
      if (maybeTimestamp.isEmpty() || maybeUuid.isEmpty()) {
        results.put(token, AuthCheckResponse.Result.INVALID);
        return;
      }
      // now that we validated signature and token age, we will also find the latest of the tokens
      // for each username
      final Long timestamp = maybeTimestamp.get();
      final UUID uuid = maybeUuid.get();
      tokenToUuid.put(token, Pair.of(uuid, timestamp));
      final Long latestTimestamp = uuidToLatestTimestamp.getOrDefault(uuid, 0L);
      if (timestamp > latestTimestamp) {
        uuidToLatestTimestamp.put(uuid, timestamp);
      }
    });

    // as a result of the first pass we now have some tokens that are marked invalid,
    // and for others we now know if for any username the list contains multiple tokens
    // we also know all distinct usernames from the list

    // if it so happens that all tokens are invalid -- respond right away
    if (tokenToUuid.isEmpty()) {
      return new AuthCheckResponse(results);
    }

    final Predicate<UUID> uuidMatches = accountsManager
        .getByE164(request.number())
        .map(account -> (Predicate<UUID>) candidateUuid -> account.getUuid().equals(candidateUuid))
        .orElse(candidateUuid -> false);

    // second pass will let us discard tokens that have newer versions and will also let us pick the winner (if any)
    request.passwords().forEach(token -> {
      if (results.containsKey(token)) {
        // result already calculated
        return;
      }
      final Pair<UUID, Long> uuidAndTime = requireNonNull(tokenToUuid.get(token));
      final Long latestTimestamp = requireNonNull(uuidToLatestTimestamp.get(uuidAndTime.getLeft()));
      // check if a newer version available
      if (uuidAndTime.getRight() < latestTimestamp) {
        results.put(token, AuthCheckResponse.Result.INVALID);
        return;
      }
      results.put(token, uuidMatches.test(uuidAndTime.getLeft())
          ? AuthCheckResponse.Result.MATCH
          : AuthCheckResponse.Result.NO_MATCH);
    });

    return new AuthCheckResponse(results);
  }
}
