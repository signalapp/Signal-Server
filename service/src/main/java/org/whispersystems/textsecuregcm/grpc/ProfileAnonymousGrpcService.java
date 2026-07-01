/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import java.time.Clock;
import java.util.Base64;
import java.util.Optional;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.FailedZkAuthentication;
import org.signal.chat.errors.NotFound;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.DeleteAvatarRequest;
import org.signal.chat.profile.DeleteAvatarResponse;
import org.signal.chat.profile.ExtendAvatarTTLRequest;
import org.signal.chat.profile.ExtendAvatarTTLResponse;
import org.signal.chat.profile.GetAvatarUploadFormRequest;
import org.signal.chat.profile.GetAvatarUploadFormResponse;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousResponse;
import org.signal.chat.profile.GetProfileAnonymousRequest;
import org.signal.chat.profile.GetProfileAnonymousResponse;
import org.signal.chat.profile.SimpleProfileAnonymousGrpc;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialPresentation;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.util.ProfileHelper;

public class ProfileAnonymousGrpcService extends SimpleProfileAnonymousGrpc.ProfileAnonymousImplBase {
  private final AccountsManager accountsManager;
  private final ProfilesManager profilesManager;
  private final ProfileBadgeConverter profileBadgeConverter;
  private final ServerZkProfileOperations zkProfileOperations;
  private final GenericServerSecretParams genericServerSecretParams;
  private final GroupSendTokenUtil groupSendTokenUtil;

  private final PostPolicyGenerator policyGenerator;

  private final RateLimiters rateLimiters;

  private final Clock clock;

  public ProfileAnonymousGrpcService(
      final AccountsManager accountsManager,
      final ProfilesManager profilesManager,
      final ProfileBadgeConverter profileBadgeConverter,
      final PostPolicyGenerator policyGenerator,
      final GenericServerSecretParams genericServerSecretParams,
      final ServerSecretParams serverSecretParams,
      final RateLimiters rateLimiters,
      final Clock clock) {
    this(accountsManager,
        profilesManager,
        profileBadgeConverter,
        policyGenerator,
        genericServerSecretParams,
        rateLimiters,
        clock,
        new ServerZkProfileOperations(serverSecretParams),
        new GroupSendTokenUtil(serverSecretParams, clock));
  }

  @VisibleForTesting
  ProfileAnonymousGrpcService(final AccountsManager accountsManager,
      final ProfilesManager profilesManager,
      final ProfileBadgeConverter profileBadgeConverter,
      final PostPolicyGenerator policyGenerator,
      final GenericServerSecretParams genericServerSecretParams,
      final RateLimiters rateLimiters,
      final Clock clock,
      final ServerZkProfileOperations zkProfileOperations,
      final GroupSendTokenUtil groupSendTokenUtil) {
    this.accountsManager = accountsManager;
    this.profilesManager = profilesManager;
    this.profileBadgeConverter = profileBadgeConverter;
    this.policyGenerator = policyGenerator;
    this.genericServerSecretParams = genericServerSecretParams;
    this.rateLimiters = rateLimiters;
    this.clock = clock;
    this.zkProfileOperations = zkProfileOperations;
    this.groupSendTokenUtil = groupSendTokenUtil;
  }

  @Override
  public GetProfileAnonymousResponse getProfile(final GetProfileAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier = GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getAccountIdentifier());

    final Optional<Account> targetAccount = accountsManager.getByServiceIdentifier(targetIdentifier);

    final boolean authorized = switch (request.getAuthenticationCase()) {
      case GROUP_SEND_TOKEN -> groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), targetIdentifier);
      case UNIDENTIFIED_ACCESS_KEY ->
          targetAccount.map(a -> UnidentifiedAccessUtil.checkUnidentifiedAccess(a, request.getUnidentifiedAccessKey().toByteArray()))
              .orElse(false);
      default -> throw GrpcExceptions.invalidArguments("invalid authentication");
    };

    if (!authorized) {
      return GetProfileAnonymousResponse.newBuilder()
          .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
          .build();
    }
    final byte[] version = request.getRequest().getVersion().toByteArray();

    return targetAccount.flatMap(account -> ProfileGrpcHelper
            .getProfile(account, profilesManager, profileBadgeConverter, version)

            // If the etag matches, drop the result
            .map(profileResult -> request.getRequest().getEtag().equals(profileResult.getEtag())
                ? GetProfileAnonymousResponse.newBuilder().setEtagMatched(true).build()
                : GetProfileAnonymousResponse.newBuilder().setProfile(profileResult).build())

            // If we didn't find a v2 profile, try a v1 profile
            .or(() -> ProfileGrpcHelper
                .getProfileV1(account, profilesManager, profileBadgeConverter, version)
                .map(v1Result -> GetProfileAnonymousResponse.newBuilder().setProfileV1(v1Result).build())))
        .orElseGet(() -> GetProfileAnonymousResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build());
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

  @Override
  public GetAvatarUploadFormResponse getAvatarUploadForm(final GetAvatarUploadFormRequest request) throws RateLimitExceededException {
    final AvatarUploadCredentialPresentation presentation;

    try {
      presentation = new AvatarUploadCredentialPresentation(
          request.getAvatarCredentialsPresentation().toByteArray());

      presentation.verify(clock.instant(), this.genericServerSecretParams);

    } catch (InvalidInputException _) {
      throw GrpcExceptions.invalidArguments("invalid credential presentation");

    } catch (VerificationFailedException _) {
      return GetAvatarUploadFormResponse.newBuilder()
          .setInvalidCredentialsPresentation(FailedZkAuthentication.getDefaultInstance())
          .build();
    }

    final byte[] identity = presentation.getCommitment();

    rateLimiters.getProfileAvatarBytesLimiter().validate(Base64.getEncoder().encodeToString(identity), request.getUploadLength());

    final String avatar = ProfileHelper.generateAvatarObjectName();

    profilesManager.setAvatarForIdentity(identity, avatar);

    return GetAvatarUploadFormResponse.newBuilder()
        .setAvatarUploadForm(ProfileGrpcHelper.generateAvatarUploadForm(avatar, request.getUploadLength(), policyGenerator, clock))
        .build();
  }

  @Override
  public ExtendAvatarTTLResponse extendAvatarTTL(final ExtendAvatarTTLRequest request) {
    final AvatarUploadCredentialPresentation presentation;
    try {
      presentation = new AvatarUploadCredentialPresentation(
          request.getAvatarCredentialsPresentation().toByteArray());
    } catch (InvalidInputException _) {
      throw GrpcExceptions.invalidArguments("invalid credential presentation");
    }

    try {
      presentation.verify(clock.instant(), this.genericServerSecretParams);

    } catch (VerificationFailedException _) {
      return ExtendAvatarTTLResponse.newBuilder()
          .setInvalidCredentialsPresentation(FailedZkAuthentication.getDefaultInstance())
          .build();
    }

    final byte[] identity = presentation.getCommitment();

    return profilesManager.extendAvatarTtlForIdentity(identity)
        .map(extendedPath -> ExtendAvatarTTLResponse.newBuilder()
            .setPath(extendedPath)
            .build())
        .orElseGet(() -> ExtendAvatarTTLResponse.newBuilder()
            .setNotFound(NotFound.getDefaultInstance())
            .build());
  }

  @Override
  public DeleteAvatarResponse deleteAvatar(final DeleteAvatarRequest request) {
    final AvatarUploadCredentialPresentation presentation;
    try {
      presentation = new AvatarUploadCredentialPresentation(
          request.getAvatarCredentialsPresentation().toByteArray());
    } catch (InvalidInputException _) {
      throw GrpcExceptions.invalidArguments("invalid credential presentation");
    }

    try {
      presentation.verify(clock.instant(), this.genericServerSecretParams);

    } catch (VerificationFailedException _) {
      return DeleteAvatarResponse.newBuilder()
          .setInvalidCredentialsPresentation(FailedZkAuthentication.getDefaultInstance())
          .build();
    }

    final byte[] identity = presentation.getCommitment();

    profilesManager.deleteAvatarForIdentity(identity);

    return DeleteAvatarResponse.newBuilder().setSuccess(Empty.getDefaultInstance()).build();
  }
}
