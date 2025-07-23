/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Clock;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialResponse;
import org.signal.chat.profile.GetUnversionedProfileAnonymousRequest;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.GetVersionedProfileAnonymousRequest;
import org.signal.chat.profile.GetVersionedProfileResponse;
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
    this.accountsManager = accountsManager;
    this.profilesManager = profilesManager;
    this.profileBadgeConverter = profileBadgeConverter;
    this.zkProfileOperations = new ServerZkProfileOperations(serverSecretParams);
    this.groupSendTokenUtil = new GroupSendTokenUtil(serverSecretParams, Clock.systemUTC());
  }

  @Override
  public GetUnversionedProfileResponse getUnversionedProfile(final GetUnversionedProfileAnonymousRequest request) throws StatusException {
    final ServiceIdentifier targetIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getServiceIdentifier());

    // Callers must be authenticated to request unversioned profiles by PNI
    if (targetIdentifier.identityType() == IdentityType.PNI) {
      throw Status.UNAUTHENTICATED.asException();
    }

    final Account account = switch (request.getAuthenticationCase()) {
      case GROUP_SEND_TOKEN -> {
        groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), targetIdentifier);

        yield accountsManager.getByServiceIdentifier(targetIdentifier)
            .orElseThrow(Status.NOT_FOUND::asException);
      }
      case UNIDENTIFIED_ACCESS_KEY ->
          getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray());
      default -> throw Status.INVALID_ARGUMENT.asException();
    };

    return ProfileGrpcHelper.buildUnversionedProfileResponse(targetIdentifier,
            null,
            account,
            profileBadgeConverter);
  }

  @Override
  public GetVersionedProfileResponse getVersionedProfile(final GetVersionedProfileAnonymousRequest request) throws StatusException {
    final ServiceIdentifier targetIdentifier = ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    if (targetIdentifier.identityType() != IdentityType.ACI) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected ACI service identifier").asException();
    }

    final Account targetAccount = getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray());
    return ProfileGrpcHelper.getVersionedProfile(targetAccount, profilesManager, request.getRequest().getVersion());
  }

  @Override
  public GetExpiringProfileKeyCredentialResponse getExpiringProfileKeyCredential(
      final GetExpiringProfileKeyCredentialAnonymousRequest request) throws StatusException {
    final ServiceIdentifier targetIdentifier = ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    if (targetIdentifier.identityType() != IdentityType.ACI) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected ACI service identifier").asException();
    }

    if (request.getRequest().getCredentialType() != CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected expiring profile key credential type").asException();
    }

    final Account account = getTargetAccountAndValidateUnidentifiedAccess(
        targetIdentifier, request.getUnidentifiedAccessKey().toByteArray());
    return ProfileGrpcHelper.getExpiringProfileKeyCredentialResponse(account.getUuid(),
            request.getRequest().getVersion(), request.getRequest().getCredentialRequest().toByteArray(), profilesManager, zkProfileOperations);
  }

  private Account getTargetAccountAndValidateUnidentifiedAccess(final ServiceIdentifier targetIdentifier, final byte[] unidentifiedAccessKey) throws StatusException {

    return accountsManager.getByServiceIdentifier(targetIdentifier)
        .filter(targetAccount -> UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, unidentifiedAccessKey))
        .orElseThrow(Status.UNAUTHENTICATED::asException);
  }
}
