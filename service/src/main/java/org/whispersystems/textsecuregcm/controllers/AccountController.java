/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.DeviceName;
import org.whispersystems.textsecuregcm.entities.EncryptedUsername;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameLinkHandle;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/accounts")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Account")
public class AccountController {
  public static final int MAXIMUM_USERNAME_HASHES_LIST_LENGTH = 20;
  public static final int USERNAME_HASH_LENGTH = 32;
  public static final int MAXIMUM_USERNAME_CIPHERTEXT_LENGTH = 128;

  private final AccountsManager accounts;
  private final RateLimiters rateLimiters;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final UsernameHashZkProofVerifier usernameHashZkProofVerifier;

  public AccountController(
      AccountsManager accounts,
      RateLimiters rateLimiters,
      RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      UsernameHashZkProofVerifier usernameHashZkProofVerifier) {
    this.accounts = accounts;
    this.rateLimiters = rateLimiters;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.usernameHashZkProofVerifier = usernameHashZkProofVerifier;
  }

  @PUT
  @Path("/gcm/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void setGcmRegistrationId(@Auth AuthenticatedDevice auth,
      @NotNull @Valid GcmRegistrationId registrationId) {

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final Device device = account.getDevice(auth.deviceId())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    if (Objects.equals(device.getGcmId(), registrationId.gcmRegistrationId())) {
      return;
    }

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(null);
      d.setGcmId(registrationId.gcmRegistrationId());
      d.setFetchesMessages(false);
    });
  }

  @DELETE
  @Path("/gcm/")
  public void deleteGcmRegistrationId(@Auth AuthenticatedDevice auth) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final Device device = account.getDevice(auth.deviceId())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    accounts.updateDevice(account, device.getId(), d -> {
      d.setGcmId(null);
      d.setFetchesMessages(false);
      d.setUserAgent("OWA");
    });
  }

  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void setApnRegistrationId(@Auth AuthenticatedDevice auth,
      @NotNull @Valid ApnRegistrationId registrationId) {

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final Device device = account.getDevice(auth.deviceId())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    // Unlike FCM tokens, we need current "last updated" timestamps for APNs tokens and so update device records
    // unconditionally
    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(registrationId.apnRegistrationId());
      d.setGcmId(null);
      d.setFetchesMessages(false);
    });
  }

  @DELETE
  @Path("/apn/")
  public void deleteApnRegistrationId(@Auth AuthenticatedDevice auth) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final Device device = account.getDevice(auth.deviceId())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(null);
      d.setFetchesMessages(false);
      if (d.getId() == 1) {
        d.setUserAgent("OWI");
      } else {
        d.setUserAgent("OWP");
      }
    });
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/registration_lock")
  public void setRegistrationLock(@Auth AuthenticatedDevice auth, @NotNull @Valid RegistrationLock accountLock) {
    final SaltedTokenHash credentials = SaltedTokenHash.generateFor(accountLock.getRegistrationLock());

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    accounts.update(account,
        a -> a.setRegistrationLock(credentials.hash(), credentials.salt()));
  }

  @DELETE
  @Path("/registration_lock")
  public void removeRegistrationLock(@Auth AuthenticatedDevice auth) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    accounts.update(account, a -> a.setRegistrationLock(null, null));
  }

  @PUT
  @Path("/name/")
  @Operation(summary = "Set a device's encrypted name",
  description = """
      Sets the encrypted name for the specified device. Primary devices may change the name of any device associated
      with their account, but linked devices may only change their own name. If no device ID is specified, then the
      authenticated device's ID will be used.
      """)
  @ApiResponse(responseCode = "204", description = "Device name changed successfully")
  @ApiResponse(responseCode = "404", description = "No device found with the given ID")
  @ApiResponse(responseCode = "403", description = "Not authorized to change the name of the device with the given ID")
  public void setName(@Auth final AuthenticatedDevice auth,
      @NotNull @Valid final DeviceName deviceName,

      @Nullable
      @QueryParam("deviceId")
      @Schema(description = "The ID of the device for which to set a name; if omitted, the authenticated device will be targeted for a name change",
          requiredMode = Schema.RequiredMode.NOT_REQUIRED)
      final Byte deviceId) {

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final byte targetDeviceId = deviceId == null ? auth.deviceId() : deviceId;

    if (account.getDevice(targetDeviceId).isEmpty()) {
      throw new NotFoundException();
    }

    final boolean mayChangeName = auth.deviceId() == Device.PRIMARY_ID || auth.deviceId() == targetDeviceId;

    if (!mayChangeName) {
      throw new ForbiddenException();
    }

    accounts.updateDevice(account, targetDeviceId, d -> d.setName(deviceName.deviceName()));
  }

  @PUT
  @Path("/attributes/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void setAccountAttributes(
      @Auth AuthenticatedDevice auth,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) String userAgent,
      @NotNull @Valid AccountAttributes attributes) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    final Account updatedAccount = accounts.update(account, a -> {
      a.getDevice(auth.deviceId()).ifPresent(d -> {
        d.setFetchesMessages(attributes.getFetchesMessages());
        d.setName(attributes.getName());
        d.setLastSeen(Util.todayInMillis());
        d.setCapabilities(attributes.getCapabilities());
        if (StringUtils.isNotBlank(userAgent)) {
          d.setUserAgent(userAgent);
        }
      });

      a.setRegistrationLockFromAttributes(attributes);
      a.setUnidentifiedAccessKey(attributes.getUnidentifiedAccessKey());
      a.setUnrestrictedUnidentifiedAccess(attributes.isUnrestrictedUnidentifiedAccess());
      a.setDiscoverableByPhoneNumber(attributes.isDiscoverableByPhoneNumber());
    });

    // if registration recovery password was sent to us, store it (or refresh its expiration)
    attributes.recoveryPassword().ifPresent(registrationRecoveryPassword ->
        registrationRecoveryPasswordsManager.store(updatedAccount.getIdentifier(IdentityType.PNI), registrationRecoveryPassword));
  }

  @GET
  @Path("/whoami")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse whoAmI(@Auth final AuthenticatedDevice auth) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    return AccountIdentityResponseBuilder.fromAccount(account);
  }

  @DELETE
  @Path("/username_hash")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Delete username hash",
      description = """
          Authenticated endpoint. Deletes previously stored username for the account.
          """
  )
  @ApiResponse(responseCode = "204", description = "Username successfully deleted.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  public CompletableFuture<Response> deleteUsernameHash(@Auth final AuthenticatedDevice auth) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    return accounts.clearUsernameHash(account)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  @PUT
  @Path("/username_hash/reserve")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Reserve username hash",
      description = """
          Authenticated endpoint. Takes in a list of hashes of potential username hashes, finds one that is not taken,
          and reserves it for the current account.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username hash reserved successfully.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "409", description = "All username hashes from the list are taken.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public CompletableFuture<ReserveUsernameHashResponse> reserveUsernameHash(
      @Auth final AuthenticatedDevice auth,
      @NotNull @Valid final ReserveUsernameHashRequest usernameRequest) throws RateLimitExceededException {

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    rateLimiters.getUsernameReserveLimiter().validate(auth.accountIdentifier());

    for (final byte[] hash : usernameRequest.usernameHashes()) {
      if (hash.length != USERNAME_HASH_LENGTH) {
        throw new WebApplicationException(Response.status(422).build());
      }
    }

    return accounts.reserveUsernameHash(account, usernameRequest.usernameHashes())
        .thenApply(reservation -> new ReserveUsernameHashResponse(reservation.reservedUsernameHash()))
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof UsernameHashNotAvailableException) {
            throw new WebApplicationException(Status.CONFLICT);
          }

          throw ExceptionUtils.wrap(throwable);
        });
  }

  @PUT
  @Path("/username_hash/confirm")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Confirm username hash",
      description = """
          Authenticated endpoint. For a previously reserved username hash, confirm that this username hash is now taken
          by this account.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username hash confirmed successfully.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "409", description = "Given username hash doesn't match the reserved one or no reservation found.")
  @ApiResponse(responseCode = "410", description = "Username hash not available (username can't be used).")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public CompletableFuture<UsernameHashResponse> confirmUsernameHash(
      @Auth final AuthenticatedDevice auth,
      @NotNull @Valid final ConfirmUsernameHashRequest confirmRequest) {

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    try {
      usernameHashZkProofVerifier.verifyProof(confirmRequest.zkProof(), confirmRequest.usernameHash());
    } catch (final BaseUsernameException e) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return rateLimiters.getUsernameSetLimiter().validateAsync(account.getUuid())
        .thenCompose(ignored -> accounts.confirmReservedUsernameHash(
            account,
            confirmRequest.usernameHash(),
            confirmRequest.encryptedUsername()))
        .thenApply(updatedAccount -> new UsernameHashResponse(updatedAccount.getUsernameHash()
            .orElseThrow(() -> new IllegalStateException("Could not get username after setting")),
            updatedAccount.getUsernameLinkHandle()))
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof UsernameReservationNotFoundException) {
            throw new WebApplicationException(Status.CONFLICT);
          }

          if (ExceptionUtils.unwrap(throwable) instanceof UsernameHashNotAvailableException) {
            throw new WebApplicationException(Status.GONE);
          }

          throw ExceptionUtils.wrap(throwable);
        })
        .toCompletableFuture();
  }

  @GET
  @Path("/username_hash/{usernameHash}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LOOKUP)
  @Operation(
      summary = "Lookup username hash",
      description = """
          Forced unauthenticated endpoint. For the given username hash, look up a user ID.
          """
  )
  @ApiResponse(responseCode = "200", description = "Account found for the given username.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Account not found for the given username.")
  public CompletableFuture<AccountIdentifierResponse> lookupUsernameHash(
      @Auth final Optional<AuthenticatedDevice> maybeAuthenticatedAccount,
      @PathParam("usernameHash") final String usernameHash) {

    requireNotAuthenticated(maybeAuthenticatedAccount);
    final byte[] hash;
    try {
      hash = Base64.getUrlDecoder().decode(usernameHash);
    } catch (IllegalArgumentException | AssertionError e) {
      throw new WebApplicationException(Response.status(422).build());
    }

    if (hash.length != USERNAME_HASH_LENGTH) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return accounts.getByUsernameHash(hash).thenApply(maybeAccount -> maybeAccount.map(Account::getUuid)
        .map(AciServiceIdentifier::new)
        .map(AccountIdentifierResponse::new)
        .orElseThrow(() -> new WebApplicationException(Status.NOT_FOUND)));
  }

  @PUT
  @Path("/username_link")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Set username link",
      description = """
          Authenticated endpoint. For the given encrypted username generates a username link handle.
          The username link handle can be used to lookup the encrypted username.
          An account can only have one username link at a time; this endpoint overwrites the previous encrypted username if there was one.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username Link updated successfully.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "409", description = "Username is not set for the account.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public UsernameLinkHandle updateUsernameLink(
      @Auth final AuthenticatedDevice auth,
      @NotNull @Valid final EncryptedUsername encryptedUsername) throws RateLimitExceededException {

    // check ratelimiter for username link operations
    rateLimiters.forDescriptor(RateLimiters.For.USERNAME_LINK_OPERATION).validate(auth.accountIdentifier());

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    // check if username hash is set for the account
    if (account.getUsernameHash().isEmpty()) {
      throw new WebApplicationException(Status.CONFLICT);
    }

    final UUID usernameLinkHandle;
    if (encryptedUsername.keepLinkHandle() && account.getUsernameLinkHandle() != null) {
      usernameLinkHandle = account.getUsernameLinkHandle();
    } else {
      usernameLinkHandle = UUID.randomUUID();
    }
    updateUsernameLink(account, usernameLinkHandle, encryptedUsername.usernameLinkEncryptedValue());
    return new UsernameLinkHandle(usernameLinkHandle);
  }

  @DELETE
  @Path("/username_link")
  @Operation(
      summary = "Delete username link",
      description = """
          Authenticated endpoint. Deletes username link for the given account: previously store encrypted username is deleted
          and username link handle is deactivated.
          """
  )
  @ApiResponse(responseCode = "204", description = "Username Link successfully deleted.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public void deleteUsernameLink(@Auth final AuthenticatedDevice auth) throws RateLimitExceededException {
    // check ratelimiter for username link operations
    rateLimiters.forDescriptor(RateLimiters.For.USERNAME_LINK_OPERATION).validate(auth.accountIdentifier());

    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    clearUsernameLink(account);
  }

  @GET
  @Path("/username_link/{uuid}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.For.USERNAME_LINK_LOOKUP_PER_IP)
  @Operation(
      summary = "Lookup username link",
      description = """
          Enforced unauthenticated endpoint. For the given username link handle, looks up the database for an associated encrypted username.
          If found, encrypted username is returned, otherwise responds with 404 Not Found.
          """
  )
  @ApiResponse(responseCode = "200", description = "Username link with the given handle was found.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "Username link was not found for the given handle.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Ratelimited.")
  public CompletableFuture<EncryptedUsername> lookupUsernameLink(
      @Auth final Optional<AuthenticatedDevice> maybeAuthenticatedAccount,
      @PathParam("uuid") final UUID usernameLinkHandle) {

    requireNotAuthenticated(maybeAuthenticatedAccount);

    return accounts.getByUsernameLinkHandle(usernameLinkHandle)
        .thenApply(maybeAccount -> maybeAccount.flatMap(Account::getEncryptedUsername)
            .map(EncryptedUsername::new)
            .orElseThrow(NotFoundException::new));
  }

  @Operation(
      summary = "Check whether an account exists",
      description = """
          Enforced unauthenticated endpoint. Checks whether an account with a given identifier exists.
          """
  )
  @ApiResponse(responseCode = "200", description = "An account with the given identifier was found.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Request must not be authenticated.")
  @ApiResponse(responseCode = "404", description = "An account was not found for the given identifier.")
  @ApiResponse(responseCode = "422", description = "Invalid request format.")
  @ApiResponse(responseCode = "429", description = "Rate-limited.")
  @HEAD
  @Path("/account/{identifier}")
  @RateLimitedByIp(RateLimiters.For.CHECK_ACCOUNT_EXISTENCE)
  public Response accountExists(
      @Auth final Optional<AuthenticatedDevice> authenticatedAccount,

      @Parameter(description = "An ACI or PNI account identifier to check")
      @PathParam("identifier") final ServiceIdentifier accountIdentifier) {

    // Disallow clients from making authenticated requests to this endpoint
    requireNotAuthenticated(authenticatedAccount);

    final Optional<Account> maybeAccount = accounts.getByServiceIdentifier(accountIdentifier);

    return Response.status(maybeAccount.map(ignored -> Status.OK).orElse(Status.NOT_FOUND)).build();
  }

  @DELETE
  @Path("/me")
  public CompletableFuture<Response> deleteAccount(@Auth AuthenticatedDevice auth) {
    final Account account = accounts.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Status.UNAUTHORIZED));

    return accounts.delete(account, AccountsManager.DeletionReason.USER_REQUEST).thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  private void clearUsernameLink(final Account account) {
    updateUsernameLink(account, null, null);
  }

  private void updateUsernameLink(
      final Account account,
      @Nullable final UUID usernameLinkHandle,
      @Nullable final byte[] encryptedUsername) {
    if ((encryptedUsername == null) ^ (usernameLinkHandle == null)) {
      throw new IllegalStateException("Both or neither arguments must be null");
    }
    accounts.update(account, a -> a.setUsernameLinkDetails(usernameLinkHandle, encryptedUsername));
  }

  private void requireNotAuthenticated(final Optional<AuthenticatedDevice> authenticatedAccount) {
    if (authenticatedAccount.isPresent()) {
      throw new BadRequestException("Operation requires unauthenticated access");
    }
  }
}
