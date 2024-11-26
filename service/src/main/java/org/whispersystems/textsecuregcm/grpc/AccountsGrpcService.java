/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.signal.chat.account.ClearRegistrationLockRequest;
import org.signal.chat.account.ClearRegistrationLockResponse;
import org.signal.chat.account.ConfigureUnidentifiedAccessRequest;
import org.signal.chat.account.ConfigureUnidentifiedAccessResponse;
import org.signal.chat.account.ConfirmUsernameHashRequest;
import org.signal.chat.account.ConfirmUsernameHashResponse;
import org.signal.chat.account.DeleteAccountRequest;
import org.signal.chat.account.DeleteAccountResponse;
import org.signal.chat.account.DeleteUsernameHashRequest;
import org.signal.chat.account.DeleteUsernameHashResponse;
import org.signal.chat.account.DeleteUsernameLinkRequest;
import org.signal.chat.account.DeleteUsernameLinkResponse;
import org.signal.chat.account.GetAccountIdentityRequest;
import org.signal.chat.account.GetAccountIdentityResponse;
import org.signal.chat.account.ReactorAccountsGrpc;
import org.signal.chat.account.ReserveUsernameHashError;
import org.signal.chat.account.ReserveUsernameHashErrorType;
import org.signal.chat.account.ReserveUsernameHashRequest;
import org.signal.chat.account.ReserveUsernameHashResponse;
import org.signal.chat.account.SetDiscoverableByPhoneNumberRequest;
import org.signal.chat.account.SetDiscoverableByPhoneNumberResponse;
import org.signal.chat.account.SetRegistrationLockRequest;
import org.signal.chat.account.SetRegistrationLockResponse;
import org.signal.chat.account.SetRegistrationRecoveryPasswordRequest;
import org.signal.chat.account.SetRegistrationRecoveryPasswordResponse;
import org.signal.chat.account.SetUsernameLinkRequest;
import org.signal.chat.account.SetUsernameLinkResponse;
import org.signal.chat.common.AccountIdentifiers;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.entities.EncryptedUsername;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;
import reactor.core.publisher.Mono;

public class AccountsGrpcService extends ReactorAccountsGrpc.AccountsImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;
  private final UsernameHashZkProofVerifier usernameHashZkProofVerifier;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  public AccountsGrpcService(final AccountsManager accountsManager,
      final RateLimiters rateLimiters,
      final UsernameHashZkProofVerifier usernameHashZkProofVerifier,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager) {

    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
    this.usernameHashZkProofVerifier = usernameHashZkProofVerifier;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
  }

  @Override
  public Mono<GetAccountIdentityResponse> getAccountIdentity(final GetAccountIdentityRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .map(account -> {
          final AccountIdentifiers.Builder accountIdentifiersBuilder = AccountIdentifiers.newBuilder()
              .addServiceIdentifiers(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(account.getUuid())))
              .addServiceIdentifiers(ServiceIdentifierUtil.toGrpcServiceIdentifier(new PniServiceIdentifier(account.getPhoneNumberIdentifier())))
              .setE164(account.getNumber());

          account.getUsernameHash().ifPresent(usernameHash ->
              accountIdentifiersBuilder.setUsernameHash(ByteString.copyFrom(usernameHash)));

          return GetAccountIdentityResponse.newBuilder()
              .setAccountIdentifiers(accountIdentifiersBuilder.build())
              .build();
        });
  }

  @Override
  public Mono<DeleteAccountResponse> deleteAccount(final DeleteAccountRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedPrimaryDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.delete(account, AccountsManager.DeletionReason.USER_REQUEST)))
        .thenReturn(DeleteAccountResponse.newBuilder().build());
  }

  @Override
  public Mono<SetRegistrationLockResponse> setRegistrationLock(final SetRegistrationLockRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedPrimaryDevice();

    if (request.getRegistrationLock().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("Registration lock secret must not be empty").asRuntimeException();
    }

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> {
          // In the previous REST-based API, clients would send hex strings directly. For backward compatibility, we
          // convert the registration lock secret to a lowercase hex string before turning it into a salted hash.
          final SaltedTokenHash credentials =
              SaltedTokenHash.generateFor(HexFormat.of().withLowerCase().formatHex(request.getRegistrationLock().toByteArray()));

          return Mono.fromFuture(() -> accountsManager.updateAsync(account,
              a -> a.setRegistrationLock(credentials.hash(), credentials.salt())));
        })
        .map(ignored -> SetRegistrationLockResponse.newBuilder().build());
  }

  @Override
  public Mono<ClearRegistrationLockResponse> clearRegistrationLock(final ClearRegistrationLockRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedPrimaryDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.updateAsync(account,
            a -> a.setRegistrationLock(null, null))))
        .map(ignored -> ClearRegistrationLockResponse.newBuilder().build());
  }

  @Override
  public Mono<ReserveUsernameHashResponse> reserveUsernameHash(final ReserveUsernameHashRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (request.getUsernameHashesCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("List of username hashes must not be empty")
          .asRuntimeException();
    }

    if (request.getUsernameHashesCount() > AccountController.MAXIMUM_USERNAME_HASHES_LIST_LENGTH) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("List of username hashes may have at most %d elements, but actually had %d",
              AccountController.MAXIMUM_USERNAME_HASHES_LIST_LENGTH, request.getUsernameHashesCount()))
          .asRuntimeException();
    }

    final List<byte[]> usernameHashes = new ArrayList<>(request.getUsernameHashesCount());

    for (final ByteString usernameHash : request.getUsernameHashesList()) {
      if (usernameHash.size() != AccountController.USERNAME_HASH_LENGTH) {
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Username hash length must be %d bytes, but was actually %d",
                AccountController.USERNAME_HASH_LENGTH, usernameHash.size()))
            .asRuntimeException();
      }

      usernameHashes.add(usernameHash.toByteArray());
    }

    return rateLimiters.getUsernameReserveLimiter().validateReactive(authenticatedDevice.accountIdentifier())
        .then(Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.reserveUsernameHash(account, usernameHashes)))
        .map(reservation -> ReserveUsernameHashResponse.newBuilder()
            .setUsernameHash(ByteString.copyFrom(reservation.reservedUsernameHash()))
            .build())
        .onErrorReturn(UsernameHashNotAvailableException.class, ReserveUsernameHashResponse.newBuilder()
            .setError(ReserveUsernameHashError.newBuilder()
                .setErrorType(ReserveUsernameHashErrorType.RESERVE_USERNAME_HASH_ERROR_TYPE_NO_HASHES_AVAILABLE)
                .build())
            .build());
  }

  @Override
  public Mono<ConfirmUsernameHashResponse> confirmUsernameHash(final ConfirmUsernameHashRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (request.getUsernameHash().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Username hash must not be empty")
          .asRuntimeException();
    }

    if (request.getUsernameHash().size() != AccountController.USERNAME_HASH_LENGTH) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Username hash length must be %d bytes, but was actually %d",
              AccountController.USERNAME_HASH_LENGTH, request.getUsernameHash().size()))
          .asRuntimeException();
    }

    if (request.getZkProof().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Zero-knowledge proof must not be empty")
          .asRuntimeException();
    }

    if (request.getUsernameCiphertext().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Username ciphertext must not be empty")
          .asRuntimeException();
    }

    if (request.getUsernameCiphertext().size() > AccountController.MAXIMUM_USERNAME_CIPHERTEXT_LENGTH) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Username hash length must at most %d bytes, but was actually %d",
              AccountController.MAXIMUM_USERNAME_CIPHERTEXT_LENGTH, request.getUsernameCiphertext().size()))
          .asRuntimeException();
    }

    try {
      usernameHashZkProofVerifier.verifyProof(request.getZkProof().toByteArray(), request.getUsernameHash().toByteArray());
    } catch (final BaseUsernameException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Could not verify proof").asRuntimeException();
    }

    return rateLimiters.getUsernameSetLimiter().validateReactive(authenticatedDevice.accountIdentifier())
        .then(Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.confirmReservedUsernameHash(account, request.getUsernameHash().toByteArray(), request.getUsernameCiphertext().toByteArray())))
        .map(updatedAccount -> ConfirmUsernameHashResponse.newBuilder()
            .setUsernameHash(ByteString.copyFrom(updatedAccount.getUsernameHash().orElseThrow()))
            .setUsernameLinkHandle(UUIDUtil.toByteString(updatedAccount.getUsernameLinkHandle()))
            .build())
        .onErrorMap(UsernameReservationNotFoundException.class, throwable -> Status.FAILED_PRECONDITION.asRuntimeException())
        .onErrorMap(UsernameHashNotAvailableException.class, throwable -> Status.NOT_FOUND.asRuntimeException());
  }

  @Override
  public Mono<DeleteUsernameHashResponse> deleteUsernameHash(final DeleteUsernameHashRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.clearUsernameHash(account)))
        .thenReturn(DeleteUsernameHashResponse.newBuilder().build());
  }

  @Override
  public Mono<SetUsernameLinkResponse> setUsernameLink(final SetUsernameLinkRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (request.getUsernameCiphertext().isEmpty() || request.getUsernameCiphertext().size() > EncryptedUsername.MAX_SIZE) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Username ciphertext must not be empty and must be shorter than %d bytes", EncryptedUsername.MAX_SIZE))
          .asRuntimeException();
    }

    return rateLimiters.getUsernameLinkOperationLimiter().validateReactive(authenticatedDevice.accountIdentifier())
        .then(Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> {
          if (account.getUsernameHash().isEmpty()) {
            return Mono.error(Status.FAILED_PRECONDITION
                .withDescription("Account does not have a username hash")
                .asRuntimeException());
          }

          final UUID linkHandle;
          if (request.getKeepLinkHandle() && account.getUsernameLinkHandle() != null) {
            linkHandle = account.getUsernameLinkHandle();
          } else {
            linkHandle = UUID.randomUUID();
          }

          return Mono.fromFuture(() -> accountsManager.updateAsync(account, a -> a.setUsernameLinkDetails(linkHandle, request.getUsernameCiphertext().toByteArray())))
              .thenReturn(linkHandle);
        })
        .map(linkHandle -> SetUsernameLinkResponse.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle))
            .build());
  }

  @Override
  public Mono<DeleteUsernameLinkResponse> deleteUsernameLink(final DeleteUsernameLinkRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return rateLimiters.getUsernameLinkOperationLimiter().validateReactive(authenticatedDevice.accountIdentifier())
        .then(Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.updateAsync(account, a -> a.setUsernameLinkDetails(null, null))))
        .thenReturn(DeleteUsernameLinkResponse.newBuilder().build());
  }

  @Override
  public Mono<ConfigureUnidentifiedAccessResponse> configureUnidentifiedAccess(final ConfigureUnidentifiedAccessRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (!request.getAllowUnrestrictedUnidentifiedAccess() && request.getUnidentifiedAccessKey().size() != UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Unidentified access key must be %d bytes, but was actually %d",
              UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH, request.getUnidentifiedAccessKey().size()))
          .asRuntimeException();
    }

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.updateAsync(account, a -> {
          a.setUnrestrictedUnidentifiedAccess(request.getAllowUnrestrictedUnidentifiedAccess());
          a.setUnidentifiedAccessKey(request.getAllowUnrestrictedUnidentifiedAccess() ? null : request.getUnidentifiedAccessKey().toByteArray());
        })))
        .thenReturn(ConfigureUnidentifiedAccessResponse.newBuilder().build());
  }

  @Override
  public Mono<SetDiscoverableByPhoneNumberResponse> setDiscoverableByPhoneNumber(final SetDiscoverableByPhoneNumberRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> accountsManager.updateAsync(account,
            a -> a.setDiscoverableByPhoneNumber(request.getDiscoverableByPhoneNumber()))))
        .thenReturn(SetDiscoverableByPhoneNumberResponse.newBuilder().build());
  }

  @Override
  public Mono<SetRegistrationRecoveryPasswordResponse> setRegistrationRecoveryPassword(final SetRegistrationRecoveryPasswordRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    if (request.getRegistrationRecoveryPassword().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Registration recovery password must not be empty")
          .asRuntimeException();
    }

    return Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
        .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException))
        .flatMap(account -> Mono.fromFuture(() -> registrationRecoveryPasswordsManager.store(account.getIdentifier(IdentityType.PNI), request.getRegistrationRecoveryPassword().toByteArray())))
        .thenReturn(SetRegistrationRecoveryPasswordResponse.newBuilder().build());
  }
}
