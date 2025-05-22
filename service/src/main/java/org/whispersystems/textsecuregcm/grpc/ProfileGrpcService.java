/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialResponse;
import org.signal.chat.profile.GetUnversionedProfileRequest;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.GetVersionedProfileRequest;
import org.signal.chat.profile.GetVersionedProfileResponse;
import org.signal.chat.profile.ProfileAvatarUploadAttributes;
import org.signal.chat.profile.ReactorProfileGrpc;
import org.signal.chat.profile.SetProfileRequest;
import org.signal.chat.profile.SetProfileRequest.AvatarChange;
import org.signal.chat.profile.SetProfileResponse;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.ProfileHelper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ProfileGrpcService extends ReactorProfileGrpc.ProfileImplBase {

  private final Clock clock;
  private final AccountsManager accountsManager;
  private final ProfilesManager  profilesManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final Map<String, BadgeConfiguration> badgeConfigurationMap;
  private final PostPolicyGenerator policyGenerator;
  private final PolicySigner policySigner;
  private final ProfileBadgeConverter profileBadgeConverter;
  private final RateLimiters rateLimiters;
  private final ServerZkProfileOperations zkProfileOperations;

  private record AvatarData(Optional<String> currentAvatar,
                            Optional<String>  finalAvatar,
                            Optional<ProfileAvatarUploadAttributes> uploadAttributes) {}

  public ProfileGrpcService(
      final Clock clock,
      final AccountsManager accountsManager,
      final ProfilesManager profilesManager,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final BadgesConfiguration badgesConfiguration,
      final PostPolicyGenerator policyGenerator,
      final PolicySigner policySigner,
      final ProfileBadgeConverter profileBadgeConverter,
      final RateLimiters rateLimiters,
      final ServerZkProfileOperations zkProfileOperations) {
    this.clock = clock;
    this.accountsManager = accountsManager;
    this.profilesManager = profilesManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.badgeConfigurationMap = badgesConfiguration.getBadges().stream().collect(Collectors.toMap(
        BadgeConfiguration::getId, Function.identity()));
    this.policyGenerator = policyGenerator;
    this.policySigner = policySigner;
    this.profileBadgeConverter = profileBadgeConverter;
    this.rateLimiters = rateLimiters;
    this.zkProfileOperations = zkProfileOperations;
  }

  @Override
  public Mono<SetProfileResponse> setProfile(final SetProfileRequest request) {
    validateRequest(request);
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> Mono.zip(
            Mono.fromFuture(() -> accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
                .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException)),
            Mono.fromFuture(() -> profilesManager.getAsync(authenticatedDevice.accountIdentifier(), request.getVersion()))
        ))
        .doOnNext(accountAndMaybeProfile -> {
          if (!request.getPaymentAddress().isEmpty()) {
            final boolean hasDisallowedPrefix =
                dynamicConfigurationManager.getConfiguration().getPaymentsConfiguration().getDisallowedPrefixes().stream()
                    .anyMatch(prefix -> accountAndMaybeProfile.getT1().getNumber().startsWith(prefix));
            if (hasDisallowedPrefix && accountAndMaybeProfile.getT2().map(VersionedProfile::paymentAddress).isEmpty()) {
              throw Status.PERMISSION_DENIED.asRuntimeException();
            }
          }
        })
        .flatMap(accountAndMaybeProfile -> {
          final Account account = accountAndMaybeProfile.getT1();
          final Optional<String> currentAvatar = accountAndMaybeProfile.getT2().map(VersionedProfile::avatar)
              .filter(avatar -> avatar.startsWith("profiles/"));
          final AvatarData avatarData = switch (AvatarChangeUtil.fromGrpcAvatarChange(request.getAvatarChange())) {
            case AVATAR_CHANGE_UNCHANGED -> new AvatarData(currentAvatar, currentAvatar, Optional.empty());
            case AVATAR_CHANGE_CLEAR -> new AvatarData(currentAvatar, Optional.empty(), Optional.empty());
            case AVATAR_CHANGE_UPDATE -> {
              final String updateAvatarObjectName = ProfileHelper.generateAvatarObjectName();
              yield new AvatarData(currentAvatar, Optional.of(updateAvatarObjectName),
                  Optional.of(generateAvatarUploadForm(updateAvatarObjectName)));
            }
          };

          final Mono<Void> profileSetMono = Mono.fromFuture(() -> profilesManager.setAsync(account.getUuid(),
              new VersionedProfile(
                  request.getVersion(),
                  request.getName().toByteArray(),
                  avatarData.finalAvatar().orElse(null),
                  request.getAboutEmoji().toByteArray(),
                  request.getAbout().toByteArray(),
                  request.getPaymentAddress().toByteArray(),
                  request.getPhoneNumberSharing().toByteArray(),
                  request.getCommitment().toByteArray())));

          final List<Mono<?>> updates = new ArrayList<>(2);

          updates.add(Mono.fromFuture(() -> accountsManager.updateAsync(account, a -> {

            final List<AccountBadge> updatedBadges = Optional.of(request.getBadgeIdsList())
                .map(badges -> ProfileHelper.mergeBadgeIdsWithExistingAccountBadges(clock, badgeConfigurationMap, badges, a.getBadges()))
                .orElseGet(a::getBadges);

            a.setBadges(clock, updatedBadges);
            a.setCurrentProfileVersion(request.getVersion());
          })));

          if (request.getAvatarChange() != AvatarChange.AVATAR_CHANGE_UNCHANGED && avatarData.currentAvatar().isPresent()) {
            updates.add(Mono.fromFuture(() -> profilesManager.deleteAvatar(avatarData.currentAvatar.get())));
          }
          return profileSetMono.thenMany(Flux.merge(updates)).then(Mono.just(avatarData));
        })
        .map(avatarData -> avatarData.uploadAttributes()
            .map(avatarUploadAttributes -> SetProfileResponse.newBuilder().setAttributes(avatarUploadAttributes).build())
            .orElse(SetProfileResponse.newBuilder().build())
        );
  }

  @Override
  public Mono<GetUnversionedProfileResponse> getUnversionedProfile(final GetUnversionedProfileRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final ServiceIdentifier targetIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getServiceIdentifier());
    return validateRateLimitAndGetAccount(authenticatedDevice.accountIdentifier(), targetIdentifier)
        .map(targetAccount -> ProfileGrpcHelper.buildUnversionedProfileResponse(targetIdentifier,
            authenticatedDevice.accountIdentifier(),
            targetAccount,
            profileBadgeConverter));
  }

  @Override
  public Mono<GetVersionedProfileResponse> getVersionedProfile(final GetVersionedProfileRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final ServiceIdentifier targetIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getAccountIdentifier());

    if (targetIdentifier.identityType() != IdentityType.ACI) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected ACI service identifier").asRuntimeException();
    }

    return validateRateLimitAndGetAccount(authenticatedDevice.accountIdentifier(), targetIdentifier)
        .flatMap(account -> ProfileGrpcHelper.getVersionedProfile(account, profilesManager, request.getVersion()));
  }

  @Override
  public Mono<GetExpiringProfileKeyCredentialResponse> getExpiringProfileKeyCredential(
      final GetExpiringProfileKeyCredentialRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final ServiceIdentifier targetIdentifier = ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getAccountIdentifier());

    if (targetIdentifier.identityType() != IdentityType.ACI) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected ACI service identifier").asRuntimeException();
    }

    if (request.getCredentialType() != CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected expiring profile key credential type").asRuntimeException();
    }

    return validateRateLimitAndGetAccount(authenticatedDevice.accountIdentifier(), targetIdentifier)
        .flatMap(targetAccount -> ProfileGrpcHelper.getExpiringProfileKeyCredentialResponse(targetAccount.getUuid(),
              request.getVersion(), request.getCredentialRequest().toByteArray(), profilesManager, zkProfileOperations));
  }


  private Mono<Account> validateRateLimitAndGetAccount(final UUID requesterUuid,
      final ServiceIdentifier targetIdentifier) {
    return rateLimiters.getProfileLimiter().validateReactive(requesterUuid)
        .then(Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(targetIdentifier))
            .flatMap(Mono::justOrEmpty))
        .switchIfEmpty(Mono.error(Status.NOT_FOUND.asException()));
  }

  private void validateRequest(final SetProfileRequest request) {
    if (request.getVersion().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("Missing version").asRuntimeException();
    }

    if (request.getCommitment().isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("Missing profile commitment").asRuntimeException();
    }

    checkByteStringLength(request.getName(), "Invalid name length", List.of(81, 285));
    checkByteStringLength(request.getAboutEmoji(), "Invalid about emoji length", List.of(0, 60));
    checkByteStringLength(request.getAbout(), "Invalid about length", List.of(0, 156, 282, 540));
    checkByteStringLength(request.getPaymentAddress(), "Invalid mobile coin address length", List.of(0, 582));
  }

  private static void checkByteStringLength(final ByteString byteString, final String errorMessage, final List<Integer> allowedLengths) {
    final int byteStringLength = byteString.toByteArray().length;

    for (int allowedLength : allowedLengths) {
      if (byteStringLength == allowedLength) {
        return;
      }
    }

    throw Status.INVALID_ARGUMENT.withDescription(errorMessage).asRuntimeException();
  }

  private ProfileAvatarUploadAttributes generateAvatarUploadForm(final String objectName) {
    final ZonedDateTime now = ZonedDateTime.now(clock);
    final Pair<String, String> policy = policyGenerator.createFor(now, objectName, ProfileHelper.MAX_PROFILE_AVATAR_SIZE_BYTES);
    final String signature = policySigner.getSignature(now, policy.second());

    return ProfileAvatarUploadAttributes.newBuilder()
        .setPath(objectName)
        .setCredential(policy.first())
        .setAcl("private")
        .setAlgorithm("AWS4-HMAC-SHA256")
        .setDate(now.format(PostPolicyGenerator.AWS_DATE_TIME))
        .setPolicy(policy.second())
        .setSignature(ByteString.copyFrom(signature.getBytes()))
        .build();
  }
}
