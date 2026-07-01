/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.signal.chat.common.S3UploadForm;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.NotFound;
import org.signal.chat.profile.GetAvatarCredentialsRequest;
import org.signal.chat.profile.GetAvatarCredentialsResponse;
import org.signal.chat.profile.GetProfileRequest;
import org.signal.chat.profile.GetProfileResponse;
import org.signal.chat.profile.PaymentsForbiddenInRegion;
import org.signal.chat.profile.ProfilesV2CapabilityRequired;
import org.signal.chat.profile.SetProfileRequest;
import org.signal.chat.profile.SetProfileResponse;
import org.signal.chat.profile.SetProfileResult;
import org.signal.chat.profile.SetProfileV1Request.AvatarChange;
import org.signal.chat.profile.SimpleProfileGrpc;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialRequest;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.util.ProfileHelper;

public class ProfileGrpcService extends SimpleProfileGrpc.ProfileImplBase {

  private static final Logger logger = LoggerFactory.getLogger(ProfileGrpcService.class);

  private final Clock clock;
  private final AccountsManager accountsManager;
  private final ProfilesManager  profilesManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final Map<String, BadgeConfiguration> badgeConfigurationMap;
  private final PostPolicyGenerator policyGenerator;
  private final GenericServerSecretParams genericServerSecretParams;
  private final ProfileBadgeConverter profileBadgeConverter;
  private final RateLimiters rateLimiters;

  private record AvatarData(Optional<String> currentAvatar,
                            Optional<String>  finalAvatar,
                            Optional<S3UploadForm> uploadAttributes) {}

  public ProfileGrpcService(
      final Clock clock,
      final AccountsManager accountsManager,
      final ProfilesManager profilesManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final BadgesConfiguration badgesConfiguration,
      final PostPolicyGenerator policyGenerator,
      final GenericServerSecretParams genericServerSecretParams,
      final ProfileBadgeConverter profileBadgeConverter,
      final RateLimiters rateLimiters) {
    this.clock = clock;
    this.accountsManager = accountsManager;
    this.profilesManager = profilesManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.badgeConfigurationMap = badgesConfiguration.getBadges().stream().collect(Collectors.toMap(
        BadgeConfiguration::getId, Function.identity()));
    this.policyGenerator = policyGenerator;
    this.genericServerSecretParams = genericServerSecretParams;
    this.profileBadgeConverter = profileBadgeConverter;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public SetProfileResponse setProfile(final SetProfileRequest request) {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidArguments("Account not found"));

    if (!account.hasCapability(DeviceCapability.PROFILES_V2)) {
      return SetProfileResponse.newBuilder()
              .setProfilesV2CapabilityRequired(ProfilesV2CapabilityRequired.getDefaultInstance())
          .build();
    }

    validateRequest(request);

    final byte[] expectedCurrentVersion = request.getExpectedCurrentVersion().toByteArray();
    final boolean currentVersionMatchesExpected = Arrays.equals(account.getCurrentProfileVersion().orElse(new byte[0]), expectedCurrentVersion);

    if (!currentVersionMatchesExpected) {
      return SetProfileResponse.newBuilder().setExpectedVersionWriteConflict(FailedPrecondition.newBuilder()
          .setDescription("current and expected profile versions must match")
          .build()).build();
    }

    final byte[] version = request.getVersion().toByteArray();
    final Optional<VersionedProfile> maybeProfile = profilesManager.get(authenticatedDevice.accountIdentifier(),
        version);
    final Optional<VersionedProfileV1> maybeV1Profile = profilesManager.getV1(
        authenticatedDevice.accountIdentifier(), HexFormat.of().formatHex(version));

    if (!request.getPaymentAddress().isEmpty() && ProfileHelper.isPaymentAddressUpdateForbidden(account, maybeProfile, maybeV1Profile, dynamicConfigurationManager)) {
      return SetProfileResponse.newBuilder()
          .setPaymentsForbiddenInRegion(PaymentsForbiddenInRegion.getDefaultInstance())
          .build();
    }

    final Optional<String> currentAvatar = maybeV1Profile.map(VersionedProfileV1::avatar)
        .filter(avatar -> avatar.startsWith("profiles/"));

    final AvatarData avatarData = switch (AvatarChangeUtil.fromGrpcAvatarChange(request.getV1Request().getAvatarChange())) {
      case AVATAR_CHANGE_UNCHANGED -> new AvatarData(currentAvatar, currentAvatar, Optional.empty());
      case AVATAR_CHANGE_CLEAR -> new AvatarData(currentAvatar, Optional.empty(), Optional.empty());
      case AVATAR_CHANGE_UPDATE -> {
        final String updateAvatarObjectName = ProfileHelper.generateAvatarObjectName();
        yield new AvatarData(currentAvatar, Optional.of(updateAvatarObjectName),
            Optional.of(ProfileGrpcHelper.generateAvatarUploadForm(updateAvatarObjectName, ProfileHelper.MAX_PROFILE_AVATAR_SIZE_BYTES, policyGenerator, clock)));
      }
    };

    final byte[] commitment = !request.getCommitment().isEmpty()
        ? request.getCommitment().toByteArray()
        : maybeProfile.orElseThrow(IllegalStateException::new).commitment();

    final VersionedProfileV1 v1Profile = new VersionedProfileV1(
        HexFormat.of().formatHex(version),
        request.getV1Request().getName().toByteArray(),
        avatarData.finalAvatar().orElse(null),
        request.getV1Request().getAboutEmoji().toByteArray(),
        request.getV1Request().getAbout().toByteArray(),
        request.getPaymentAddress().isEmpty() ? null : request.getPaymentAddress().toByteArray(),
        request.getV1Request().getPhoneNumberSharing().toByteArray(),
        commitment);

    final VersionedProfile profile = new VersionedProfile(version,
        request.getData().toByteArray(),
        request.getPaymentAddress().isEmpty() ? null : request.getPaymentAddress().toByteArray(),
        commitment
    );

    try {
      profilesManager.set(account.getIdentifier(IdentityType.ACI), v1Profile, profile,
          request.getExpectedCurrentDataHash().isEmpty() ? null : request.getExpectedCurrentDataHash().toByteArray());

    } catch (WriteConflictException _) {
      return SetProfileResponse.newBuilder()
          .setExpectedDataWriteConflict(FailedPrecondition.newBuilder()
              .setDescription("current and expected data hash mismatch")
              .build())
          .build();
    }

    try {
      accountsManager.updateCurrentProfileVersion(account.getIdentifier(IdentityType.ACI), version, expectedCurrentVersion, a -> {

        final List<AccountBadge> updatedBadges = Optional.of(request.getBadgeIdsList())
            .map(badges -> ProfileHelper.mergeBadgeIdsWithExistingAccountBadges(clock, badgeConfigurationMap, badges,
                a.getBadges()))
            .orElseGet(a::getBadges);

        a.setBadges(clock, updatedBadges);
      });

    } catch (final WriteConflictException _) {
      return SetProfileResponse.newBuilder()
          .setExpectedVersionWriteConflict(FailedPrecondition.newBuilder()
              .setDescription("current and expected version mismatch")
              .build())
          .build();
    }

    if (request.getV1Request().getAvatarChange() != AvatarChange.AVATAR_CHANGE_UNCHANGED && avatarData.currentAvatar().isPresent()) {
      profilesManager.deleteAvatar(avatarData.currentAvatar().get());
    }

    return avatarData.uploadAttributes()
        .map(avatarUploadAttributes -> SetProfileResponse.newBuilder()
            .setResult(SetProfileResult.newBuilder()
                .setV1AvatarUploadForm(avatarUploadAttributes).build())
            .build())
        .orElse(SetProfileResponse.newBuilder()
            .setResult(SetProfileResult.getDefaultInstance())
            .build());
  }

  @Override
  public GetAvatarCredentialsResponse getAvatarCredentials(final GetAvatarCredentialsRequest request) {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("account not found"));

    if (account.getZkCredentialKey().isEmpty()) {
      return GetAvatarCredentialsResponse.newBuilder()
          .setMissingZkCredentialKey(FailedPrecondition.newBuilder().setDescription("account requires ZK credential key"))
          .build();
    }

    try {
      final AvatarUploadCredentialRequest credentialRequest = new AvatarUploadCredentialRequest(
          request.getAvatarCredentialsRequest().toByteArray());

      final AvatarUploadCredentialResponse credentialResponse = credentialRequest.issueCredential(
          new ServiceId.Aci(account.getIdentifier(IdentityType.ACI)),
          account.getZkCredentialKey().get(),
          Objects.requireNonNull(account.getZkCredentialKeyRotationId()),
          clock.instant().truncatedTo(ChronoUnit.DAYS),
          this.genericServerSecretParams);

      return GetAvatarCredentialsResponse.newBuilder()
          .setAvatarCredentials(ByteString.copyFrom(credentialResponse.serialize()))
          .build();
    } catch (InvalidInputException | VerificationFailedException _) {
      throw GrpcExceptions.invalidArguments("invalid credential request");
    }
  }

  @Override
  public GetProfileResponse getProfile(final GetProfileRequest request) throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final ServiceIdentifier targetIdentifier =
        GrpcServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getAccountIdentifier());
    final byte[] version = request.getVersion().toByteArray();
    final Optional<Account> maybeAccount =
        validateRateLimitAndGetAccount(authenticatedDevice.accountIdentifier(), targetIdentifier);

    return maybeAccount.flatMap(account -> ProfileGrpcHelper
            .getProfile(account, profilesManager, profileBadgeConverter, version)

            // If the etag matches, drop the result
            .map(profileResult -> request.getEtag().equals(profileResult.getEtag())
                ? GetProfileResponse.newBuilder().setEtagMatched(true).build()
                : GetProfileResponse.newBuilder().setProfile(profileResult).build())

            // If we didn't find a v2 profile, try a v1 profile
            .or(() -> ProfileGrpcHelper
                .getProfileV1(account, profilesManager, profileBadgeConverter, version)
                .map(v1Result -> GetProfileResponse.newBuilder().setLegacyProfile(v1Result).build())))
        .orElseGet(() -> GetProfileResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build());
  }

  private Optional<Account> validateRateLimitAndGetAccount(final UUID requesterUuid,
      final ServiceIdentifier targetIdentifier) throws RateLimitExceededException {
    rateLimiters.getProfileLimiter().validate(requesterUuid);

    return accountsManager.getByServiceIdentifier(targetIdentifier);
  }

  private void validateRequest(final SetProfileRequest request) {
    if (request.getExpectedCurrentDataHash().isEmpty() && request.getCommitment().isEmpty()) {
      throw GrpcExceptions.invalidArguments("At least one of expected current data hash and commitment is required");
    }

    // v1 -> v2 migration
    if (request.getCommitment().isEmpty()) {
      throw GrpcExceptions.invalidArguments("Request must include commitment during migration");
    }
  }
}
