/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import org.signal.chat.account.CheckAccountExistenceRequest;
import org.signal.chat.account.CheckAccountExistenceResponse;
import org.signal.chat.account.LookupUsernameHashRequest;
import org.signal.chat.account.LookupUsernameHashResponse;
import org.signal.chat.account.LookupUsernameLinkRequest;
import org.signal.chat.account.LookupUsernameLinkResponse;
import org.signal.chat.account.ReactorAccountsAnonymousGrpc;
import org.signal.chat.errors.NotFound;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.publisher.Mono;
import java.util.Optional;
import java.util.UUID;

public class AccountsAnonymousGrpcService extends ReactorAccountsAnonymousGrpc.AccountsAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;

  public AccountsAnonymousGrpcService(final AccountsManager accountsManager, final RateLimiters rateLimiters) {
    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public Mono<CheckAccountExistenceResponse> checkAccountExistence(final CheckAccountExistenceRequest request) {
    final ServiceIdentifier serviceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getServiceIdentifier());

    return RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getCheckAccountExistenceLimiter())
        .then(Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(serviceIdentifier)))
        .map(Optional::isPresent)
        .map(accountExists -> CheckAccountExistenceResponse.newBuilder()
            .setAccountExists(accountExists)
            .build());
  }

  @Override
  public Mono<LookupUsernameHashResponse> lookupUsernameHash(final LookupUsernameHashRequest request) {
    if (request.getUsernameHash().size() != AccountController.USERNAME_HASH_LENGTH) {
      throw GrpcExceptions.fieldViolation("username_hash",
          String.format("Illegal username hash length; expected %d bytes, but got %d bytes",
              AccountController.USERNAME_HASH_LENGTH, request.getUsernameHash().size()));
    }

    return RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getUsernameLookupLimiter())
        .then(Mono.fromFuture(() -> accountsManager.getByUsernameHash(request.getUsernameHash().toByteArray())))
        .map(maybeAccount -> maybeAccount
            .map(account -> LookupUsernameHashResponse.newBuilder()
                .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(account.getUuid())))
                .build())
            .orElseGet(() -> LookupUsernameHashResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build()));
  }

  @Override
  public Mono<LookupUsernameLinkResponse> lookupUsernameLink(final LookupUsernameLinkRequest request) {
    final UUID linkHandle;

    try {
      linkHandle = UUIDUtil.fromByteString(request.getUsernameLinkHandle());
    } catch (final IllegalArgumentException e) {
      throw GrpcExceptions.fieldViolation("username_link_handle", "Could not interpret link handle as UUID");
    }

    return RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getUsernameLinkLookupLimiter())
        .then(Mono.fromFuture(() -> accountsManager.getByUsernameLinkHandle(linkHandle)))
        .map(maybeAccount -> maybeAccount
            .flatMap(Account::getEncryptedUsername)
            .map(usernameCiphertext -> LookupUsernameLinkResponse.newBuilder()
                .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
                .build())
            .orElseGet(() -> LookupUsernameLinkResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build()));
  }
}
