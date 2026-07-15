/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.UUID;
import org.signal.chat.account.Capabilities;
import org.signal.chat.account.CheckAccountExistenceRequest;
import org.signal.chat.account.CheckAccountExistenceResponse;
import org.signal.chat.account.GetCapabilitiesAnonymousRequest;
import org.signal.chat.account.GetCapabilitiesAnonymousResponse;
import org.signal.chat.account.LookupUsernameHashRequest;
import org.signal.chat.account.LookupUsernameHashResponse;
import org.signal.chat.account.LookupUsernameLinkRequest;
import org.signal.chat.account.LookupUsernameLinkResponse;
import org.signal.chat.account.SimpleAccountsAnonymousGrpc;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.NotFound;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class AccountsAnonymousGrpcService extends SimpleAccountsAnonymousGrpc.AccountsAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;
  private final GroupSendTokenUtil groupSendTokenUtil;

  public AccountsAnonymousGrpcService(
      final AccountsManager accountsManager,
      final RateLimiters rateLimiters,
      final GroupSendTokenUtil groupSendTokenUtil) {
    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
    this.groupSendTokenUtil = groupSendTokenUtil;
  }

  @Override
  public CheckAccountExistenceResponse checkAccountExistence(final CheckAccountExistenceRequest request)
      throws RateLimitExceededException {

    final ServiceIdentifier serviceIdentifier =
        GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getServiceIdentifier());

    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getCheckAccountExistenceLimiter());

    return CheckAccountExistenceResponse.newBuilder()
        .setAccountExists(accountsManager.getByServiceIdentifier(serviceIdentifier).isPresent())
        .build();
  }

  @Override
  public LookupUsernameHashResponse lookupUsernameHash(final LookupUsernameHashRequest request)
      throws RateLimitExceededException {

    RateLimitUtil.rateLimitByRemoteAddress(rateLimiters.getUsernameLookupLimiter());

    return accountsManager.getByUsernameHash(request.getUsernameHash().toByteArray()).join()
        .map(account -> LookupUsernameHashResponse.newBuilder()
            .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(account.getUuid())))
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


  @Override
  public GetCapabilitiesAnonymousResponse getCapabilities(GetCapabilitiesAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier =
        GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getAccountIdentifier());
    final Optional<Account> targetAccount = accountsManager.getByServiceIdentifier(targetIdentifier);

    final boolean authorized = switch (request.getAuthenticationCase()) {
      case GROUP_SEND_TOKEN -> groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), targetIdentifier);
      case UNIDENTIFIED_ACCESS_KEY -> targetAccount
          .map(a -> UnidentifiedAccessUtil.checkUnidentifiedAccess(a, request.getUnidentifiedAccessKey().toByteArray()))
          .orElse(false);
      default -> throw GrpcExceptions.invalidArguments("invalid authentication");
    };

    if (!authorized) {
      return GetCapabilitiesAnonymousResponse.newBuilder()
          .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
          .build();
    }

    return targetAccount.map(target -> {
          final Capabilities.Builder builder = Capabilities.newBuilder();
          for (DeviceCapability capability : DeviceCapability.PUBLIC_VISIBLE_CAPABILITIES) {
            if (target.hasCapability(capability)) {
              builder.addCapabilities(DeviceCapabilityUtil.toGrpcDeviceCapability(capability));
            }
          }
          return GetCapabilitiesAnonymousResponse.newBuilder().setCapabilities(builder.build()).build();
        })
        .orElseGet(() -> GetCapabilitiesAnonymousResponse.newBuilder()
            .setNotFound(NotFound.getDefaultInstance())
            .build());
  }

}
