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
import org.signal.chat.profile.ReactorProfileAnonymousGrpc;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import reactor.core.publisher.Mono;

public class ProfileAnonymousGrpcService extends ReactorProfileAnonymousGrpc.ProfileAnonymousImplBase {
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
  public Mono<GetUnversionedProfileResponse> getUnversionedProfile(final GetUnversionedProfileAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getServiceIdentifier());

    // Callers must be authenticated to request unversioned profiles by PNI
    if (targetIdentifier.identityType() == IdentityType.PNI) {
      throw Status.UNAUTHENTICATED.asRuntimeException();
    }

    final Mono<Account> account = switch (request.getAuthenticationCase()) {
      case GROUP_SEND_TOKEN -> {
        try {
          groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), targetIdentifier);

          yield Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(targetIdentifier))
              .flatMap(Mono::justOrEmpty)
              .switchIfEmpty(Mono.error(Status.NOT_FOUND.asException()));
        } catch (final StatusException e) {
          yield Mono.error(e);
        }
      }
      case UNIDENTIFIED_ACCESS_KEY ->
          getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray());
      default -> Mono.error(Status.INVALID_ARGUMENT.asException());
    };

    return account.map(targetAccount -> ProfileGrpcHelper.buildUnversionedProfileResponse(targetIdentifier,
            null,
            targetAccount,
            profileBadgeConverter));
  }

  @Override
  public Mono<GetVersionedProfileResponse> getVersionedProfile(final GetVersionedProfileAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier = ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    if (targetIdentifier.identityType() != IdentityType.ACI) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected ACI service identifier").asRuntimeException();
    }

    return getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray())
        .flatMap(targetAccount -> ProfileGrpcHelper.getVersionedProfile(targetAccount, profilesManager, request.getRequest().getVersion()));
  }

  @Override
  public Mono<GetExpiringProfileKeyCredentialResponse> getExpiringProfileKeyCredential(
      final GetExpiringProfileKeyCredentialAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier = ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    if (targetIdentifier.identityType() != IdentityType.ACI) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected ACI service identifier").asRuntimeException();
    }

    if (request.getRequest().getCredentialType() != CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected expiring profile key credential type").asRuntimeException();
    }

    return getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray())
        .flatMap(account -> ProfileGrpcHelper.getExpiringProfileKeyCredentialResponse(account.getUuid(),
            request.getRequest().getVersion(), request.getRequest().getCredentialRequest().toByteArray(), profilesManager, zkProfileOperations));
  }

  private Mono<Account> getTargetAccountAndValidateUnidentifiedAccess(final ServiceIdentifier targetIdentifier, final byte[] unidentifiedAccessKey) {
    return Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(targetIdentifier))
      .flatMap(Mono::justOrEmpty)
      .filter(targetAccount -> UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, unidentifiedAccessKey))
      .switchIfEmpty(Mono.error(Status.UNAUTHENTICATED.asException()));
  }
}
