package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import org.signal.chat.profile.SetProfileRequest.AvatarChange;
import org.signal.chat.profile.ProfileAvatarUploadAttributes;
import org.signal.chat.profile.ReactorProfileGrpc;
import org.signal.chat.profile.SetProfileRequest;
import org.signal.chat.profile.SetProfileResponse;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ProfileGrpcService extends ReactorProfileGrpc.ProfileImplBase {
  private final Clock clock;

  private final ProfilesManager  profilesManager;
  private final AccountsManager accountsManager;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final Map<String, BadgeConfiguration> badgeConfigurationMap;
  private final PolicySigner policySigner;
  private final PostPolicyGenerator policyGenerator;

  private final S3AsyncClient asyncS3client;
  private final String bucket;

  private record AvatarData(Optional<String> currentAvatar,
                            Optional<String>  finalAvatar,
                            Optional<ProfileAvatarUploadAttributes> uploadAttributes) {}

  public ProfileGrpcService(
      Clock clock,
      AccountsManager accountsManager,
      ProfilesManager profilesManager,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      BadgesConfiguration badgesConfiguration,
      S3AsyncClient asyncS3client,
      PostPolicyGenerator policyGenerator,
      PolicySigner policySigner,
      String bucket) {
    this.clock = clock;
    this.accountsManager = accountsManager;
    this.profilesManager = profilesManager;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.badgeConfigurationMap = badgesConfiguration.getBadges().stream().collect(Collectors.toMap(
        BadgeConfiguration::getId, Function.identity()));
    this.bucket = bucket;
    this.asyncS3client  = asyncS3client;
    this.policyGenerator = policyGenerator;
    this.policySigner = policySigner;
  }

  @Override
  public Mono<SetProfileResponse> setProfile(SetProfileRequest request) {
    validateRequest(request);
    return Mono.fromSupplier(AuthenticationUtil::requireAuthenticatedDevice)
        .flatMap(authenticatedDevice -> Mono.zip(
            Mono.fromFuture(accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier()))
                .map(maybeAccount -> maybeAccount.orElseThrow(Status.UNAUTHENTICATED::asRuntimeException)),
            Mono.fromFuture(profilesManager.getAsync(authenticatedDevice.accountIdentifier(), request.getVersion()))
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
                  Optional.of(ProfileHelper.generateAvatarUploadFormGrpc(policyGenerator, policySigner, updateAvatarObjectName)));
            }
          };

          final Mono<Void> profileSetMono = Mono.fromFuture(profilesManager.setAsync(account.getUuid(),
              new VersionedProfile(
                  request.getVersion(),
                  request.getName().toByteArray(),
                  avatarData.finalAvatar().orElse(null),
                  request.getAboutEmoji().toByteArray(),
                  request.getAbout().toByteArray(),
                  request.getPaymentAddress().toByteArray(),
                  request.getCommitment().toByteArray())));

          final List<Mono<?>> updates = new ArrayList<>(2);
          final List<AccountBadge> updatedBadges = Optional.of(request.getBadgeIdsList())
              .map(badges -> ProfileHelper.mergeBadgeIdsWithExistingAccountBadges(clock, badgeConfigurationMap, badges, account.getBadges()))
              .orElseGet(account::getBadges);

          updates.add(Mono.fromFuture(accountsManager.updateAsync(account, a -> {
            a.setBadges(clock, updatedBadges);
            a.setCurrentProfileVersion(request.getVersion());
          })));
          if (request.getAvatarChange() != AvatarChange.AVATAR_CHANGE_UNCHANGED && avatarData.currentAvatar().isPresent()) {
            updates.add(Mono.fromFuture(asyncS3client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(avatarData.currentAvatar().get())
                .build())));
          }
          return profileSetMono.thenMany(Flux.merge(updates)).then(Mono.just(avatarData));
        })
        .map(avatarData -> avatarData.uploadAttributes()
            .map(avatarUploadAttributes -> SetProfileResponse.newBuilder().setAttributes(avatarUploadAttributes).build())
            .orElse(SetProfileResponse.newBuilder().build())
        );
  }

  private void validateRequest(SetProfileRequest request) {
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
}
