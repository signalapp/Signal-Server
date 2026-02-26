/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.util.UUID;
import org.signal.chat.account.CheckAccountExistenceRequest;
import org.signal.chat.account.CheckAccountExistenceResponse;
import org.signal.chat.account.LookupUsernameHashRequest;
import org.signal.chat.account.LookupUsernameHashResponse;
import org.signal.chat.account.LookupUsernameLinkRequest;
import org.signal.chat.account.LookupUsernameLinkResponse;
import org.signal.chat.account.SimpleAccountsAnonymousGrpc;
import org.signal.chat.errors.NotFound;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class AccountsAnonymousGrpcService extends SimpleAccountsAnonymousGrpc.AccountsAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;

  public AccountsAnonymousGrpcService(final AccountsManager accountsManager, final RateLimiters rateLimiters) {
    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public CheckAccountExistenceResponse checkAccountExistence(final CheckAccountExistenceRequest request)
      throws RateLimitExceededException {

    final ServiceIdentifier serviceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getServiceIdentifier());

    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getCheckAccountExistenceLimiter());

    return CheckAccountExistenceResponse.newBuilder()
        .setAccountExists(accountsManager.getByServiceIdentifier(serviceIdentifier).isPresent())
        .build();
  }

  @Override
  public LookupUsernameHashResponse lookupUsernameHash(final LookupUsernameHashRequest request)
      throws RateLimitExceededException {

    if (request.getUsernameHash().size() != AccountController.USERNAME_HASH_LENGTH) {
      throw GrpcExceptions.fieldViolation("username_hash",
          String.format("Illegal username hash length; expected %d bytes, but got %d bytes",
              AccountController.USERNAME_HASH_LENGTH, request.getUsernameHash().size()));
    }

    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getUsernameLookupLimiter());

    return accountsManager.getByUsernameHash(request.getUsernameHash().toByteArray()).join()
        .map(account -> LookupUsernameHashResponse.newBuilder()
            .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(account.getUuid())))
            .build())
        .orElseGet(() -> LookupUsernameHashResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build());
  }

  @Override
  public LookupUsernameLinkResponse lookupUsernameLink(final LookupUsernameLinkRequest request)
      throws RateLimitExceededException {
    final UUID linkHandle;

    try {
      linkHandle = UUIDUtil.fromByteString(request.getUsernameLinkHandle());
    } catch (final IllegalArgumentException e) {
      throw GrpcExceptions.fieldViolation("username_link_handle", "Could not interpret link handle as UUID");
    }

    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getUsernameLinkLookupLimiter());

    return accountsManager.getByUsernameLinkHandle(linkHandle).join()
        .flatMap(Account::getEncryptedUsername)
        .map(usernameCiphertext -> LookupUsernameLinkResponse.newBuilder()
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .build())
        .orElseGet(() -> LookupUsernameLinkResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build());
  }
}
