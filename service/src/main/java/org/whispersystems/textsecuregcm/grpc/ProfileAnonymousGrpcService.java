/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.time.Clock;
import java.util.Optional;
import com.google.common.annotations.VisibleForTesting;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.NotFound;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousResponse;
import org.signal.chat.profile.GetUnversionedProfileAnonymousRequest;
import org.signal.chat.profile.GetUnversionedProfileAnonymousResponse;
import org.signal.chat.profile.GetVersionedProfileAnonymousRequest;
import org.signal.chat.profile.GetVersionedProfileAnonymousResponse;
import org.signal.chat.profile.SimpleProfileAnonymousGrpc;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;

public class ProfileAnonymousGrpcService extends SimpleProfileAnonymousGrpc.ProfileAnonymousImplBase {
  private final AccountsManager accountsManager;
  private final ProfilesManager profilesManager;
  private final ProfileBadgeConverter profileBadgeConverter;
  private final ServerZkProfileOperations zkProfileOperations;
  private final GroupSendTokenUtil groupSendTokenUtil;

  public ProfileAnonymousGrpcService(
      final AccountsManager accountsManager,
      final ProfilesManager profilesManager,
      final ProfileBadgeConverter profileBadgeConverter,
      final ServerSecretParams serverSecretParams) {
    this(accountsManager,
        profilesManager,
        profileBadgeConverter,
        new ServerZkProfileOperations(serverSecretParams),
        new GroupSendTokenUtil(serverSecretParams, Clock.systemUTC()));
  }

  @VisibleForTesting
  ProfileAnonymousGrpcService(final AccountsManager accountsManager,
      final ProfilesManager profilesManager,
      final ProfileBadgeConverter profileBadgeConverter,
      final ServerZkProfileOperations zkProfileOperations,
      final GroupSendTokenUtil groupSendTokenUtil) {
    this.accountsManager = accountsManager;
    this.profilesManager = profilesManager;
    this.profileBadgeConverter = profileBadgeConverter;
    this.zkProfileOperations = zkProfileOperations;
    this.groupSendTokenUtil = groupSendTokenUtil;
  }

  @Override
  public GetUnversionedProfileAnonymousResponse getUnversionedProfile(final GetUnversionedProfileAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier =
        GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getServiceIdentifier());

    // Callers must be authenticated to request unversioned profiles by PNI
    if (targetIdentifier.identityType() == IdentityType.PNI) {
      throw GrpcExceptions.invalidArguments("aci service identifier type required");
    }

    final Optional<Account> targetAccount = accountsManager.getByServiceIdentifier(targetIdentifier);

    final boolean authorized = switch (request.getAuthenticationCase()) {
      case GROUP_SEND_TOKEN -> groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), targetIdentifier);
      case UNIDENTIFIED_ACCESS_KEY ->
          targetAccount.map(a -> UnidentifiedAccessUtil.checkUnidentifiedAccess(a, request.getUnidentifiedAccessKey().toByteArray()))
              .orElse(false);
      default -> throw GrpcExceptions.invalidArguments("invalid authentication");
    };

    if (!authorized) {
      return GetUnversionedProfileAnonymousResponse.newBuilder()
          .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
          .build();
    }

    return targetAccount.map(account ->
            GetUnversionedProfileAnonymousResponse.newBuilder()
                .setResult(ProfileGrpcHelper.buildUnversionedProfileResult(targetIdentifier,
                    null,
                    account,
                    profileBadgeConverter))
                .build())
        .orElseGet(() -> GetUnversionedProfileAnonymousResponse.newBuilder()
            .setNotFound(NotFound.getDefaultInstance())
            .build());
  }

  @Override
  public GetVersionedProfileAnonymousResponse getVersionedProfile(final GetVersionedProfileAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier = GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    final Optional<Account> targetAccount = getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray());

    return targetAccount.flatMap(account ->
        ProfileGrpcHelper.getVersionedProfile(account,
                profilesManager,
                request.getRequest().getVersion().toByteArray(),
                request.getRequest().getDataEtag().toByteArray(),
                request.getRequest().getPaymentAddressEtag().toByteArray()))
        .map(result ->
            GetVersionedProfileAnonymousResponse.newBuilder()
                .setResult(result)
                .build())
        .orElseGet(() ->
            GetVersionedProfileAnonymousResponse.newBuilder()
                .setNotFound(NotFound.getDefaultInstance())
                .build());
  }

  @Override
  public GetExpiringProfileKeyCredentialAnonymousResponse getExpiringProfileKeyCredential(
      final GetExpiringProfileKeyCredentialAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier = GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    if (request.getRequest().getCredentialType() != CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY) {
      throw GrpcExceptions.invalidArguments("invalid credential type");
    }

    final Optional<Account> maybeAccount = getTargetAccountAndValidateUnidentifiedAccess(
        targetIdentifier, request.getUnidentifiedAccessKey().toByteArray());

    return maybeAccount.map(account ->
        ProfileGrpcHelper.getExpiringProfileKeyCredentialResult(account,
                request.getRequest().getVersion().toByteArray(), request.getRequest().getCredentialRequest().toByteArray(),
                profilesManager, zkProfileOperations)
            .map(result -> GetExpiringProfileKeyCredentialAnonymousResponse.newBuilder()
                .setResult(result)
                .build())
            .orElseGet(() -> GetExpiringProfileKeyCredentialAnonymousResponse.newBuilder()
                .setNotFound(NotFound.getDefaultInstance())
                .build())).orElseGet(() -> GetExpiringProfileKeyCredentialAnonymousResponse.newBuilder()
        .setNotFound(NotFound.getDefaultInstance())
        .build());

  }

  private Optional<Account> getTargetAccountAndValidateUnidentifiedAccess(final ServiceIdentifier targetIdentifier, final byte[] unidentifiedAccessKey)  {

    return accountsManager.getByServiceIdentifier(targetIdentifier)
        .filter(targetAccount -> UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, unidentifiedAccessKey));
  }
}
