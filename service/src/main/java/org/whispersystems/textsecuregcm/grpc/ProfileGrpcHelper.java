/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.signal.chat.profile.Badge;
import org.signal.chat.profile.BadgeSvg;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialResponse;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.GetVersionedProfileResponse;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.util.ProfileHelper;
import reactor.core.publisher.Mono;

public class ProfileGrpcHelper {
  static Mono<GetVersionedProfileResponse> getVersionedProfile(final Account account,
      final ProfilesManager profilesManager,
      final String requestVersion) {
    return Mono.fromFuture(() -> profilesManager.getAsync(account.getUuid(), requestVersion))
        .map(maybeProfile -> {
          if (maybeProfile.isEmpty()) {
            throw Status.NOT_FOUND.withDescription("Profile version not found").asRuntimeException();
          }

          final GetVersionedProfileResponse.Builder responseBuilder = GetVersionedProfileResponse.newBuilder();

          maybeProfile.map(VersionedProfile::name).map(ByteString::copyFrom).ifPresent(responseBuilder::setName);
          maybeProfile.map(VersionedProfile::about).map(ByteString::copyFrom).ifPresent(responseBuilder::setAbout);
          maybeProfile.map(VersionedProfile::aboutEmoji).map(ByteString::copyFrom).ifPresent(responseBuilder::setAboutEmoji);
          maybeProfile.map(VersionedProfile::avatar).ifPresent(responseBuilder::setAvatar);
          maybeProfile.map(VersionedProfile::phoneNumberSharing).map(ByteString::copyFrom).ifPresent(responseBuilder::setPhoneNumberSharing);

          // Allow requests where either the version matches the latest version on Account or the latest version on Account
          // is empty to read the payment address.
          maybeProfile
              .filter(p -> account.getCurrentProfileVersion().map(v -> v.equals(requestVersion)).orElse(true))
              .map(VersionedProfile::paymentAddress)
              .map(ByteString::copyFrom)
              .ifPresent(responseBuilder::setPaymentAddress);

          return responseBuilder.build();
        });
  }

  @VisibleForTesting
  static List<Badge> buildBadges(final List<org.whispersystems.textsecuregcm.entities.Badge> badges) {
    final ArrayList<Badge> grpcBadges = new ArrayList<>();
    for (final org.whispersystems.textsecuregcm.entities.Badge badge : badges) {
      grpcBadges.add(Badge.newBuilder()
          .setId(badge.getId())
          .setCategory(badge.getCategory())
          .setName(badge.getName())
          .setDescription(badge.getDescription())
          .addAllSprites6(badge.getSprites6())
          .setSvg(badge.getSvg())
          .addAllSvgs(buildBadgeSvgs(badge.getSvgs()))
          .build());
    }
    return grpcBadges;
  }

  @VisibleForTesting
  static List<org.signal.chat.common.DeviceCapability> buildAccountCapabilities(final Account account) {
    return Arrays.stream(DeviceCapability.values())
        .filter(DeviceCapability::includeInProfile)
        .filter(account::hasCapability)
        .map(DeviceCapabilityUtil::toGrpcDeviceCapability)
        .toList();
  }

  private static List<BadgeSvg> buildBadgeSvgs(final List<org.whispersystems.textsecuregcm.entities.BadgeSvg> badgeSvgs) {
    ArrayList<BadgeSvg> grpcBadgeSvgs = new ArrayList<>();
    for (final org.whispersystems.textsecuregcm.entities.BadgeSvg badgeSvg : badgeSvgs) {
      grpcBadgeSvgs.add(BadgeSvg.newBuilder()
          .setDark(badgeSvg.getDark())
          .setLight(badgeSvg.getLight())
          .build());
    }
    return grpcBadgeSvgs;
  }

  static GetUnversionedProfileResponse buildUnversionedProfileResponse(
      final ServiceIdentifier targetIdentifier,
      final UUID requesterUuid,
      final Account targetAccount,
      final ProfileBadgeConverter profileBadgeConverter) {
    final GetUnversionedProfileResponse.Builder responseBuilder = GetUnversionedProfileResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(targetAccount.getIdentityKey(targetIdentifier.identityType()).serialize()))
        .addAllCapabilities(buildAccountCapabilities(targetAccount));

    switch (targetIdentifier.identityType()) {
      case ACI -> {
        responseBuilder.setUnrestrictedUnidentifiedAccess(targetAccount.isUnrestrictedUnidentifiedAccess())
            .addAllBadges(buildBadges(profileBadgeConverter.convert(
                RequestAttributesUtil.getAvailableAcceptedLocales(),
                targetAccount.getBadges(),
                ProfileHelper.isSelfProfileRequest(requesterUuid, targetIdentifier))));

        targetAccount.getUnidentifiedAccessKey()
            .map(UnidentifiedAccessChecksum::generateFor)
            .map(ByteString::copyFrom)
            .ifPresent(responseBuilder::setUnidentifiedAccess);
      }
      case PNI -> responseBuilder.setUnrestrictedUnidentifiedAccess(false);
    }

    return responseBuilder.build();
  }

  static Mono<GetExpiringProfileKeyCredentialResponse> getExpiringProfileKeyCredentialResponse(
      final UUID targetUuid,
      final String version,
      final byte[] encodedCredentialRequest,
      final ProfilesManager profilesManager,
      final ServerZkProfileOperations zkProfileOperations) {
    return Mono.fromFuture(profilesManager.getAsync(targetUuid, version))
        .flatMap(Mono::justOrEmpty)
        .map(profile -> {
          final ExpiringProfileKeyCredentialResponse profileKeyCredentialResponse;
          try {
            profileKeyCredentialResponse = ProfileHelper.getExpiringProfileKeyCredential(encodedCredentialRequest,
                profile, new ServiceId.Aci(targetUuid), zkProfileOperations);
          } catch (VerificationFailedException | InvalidInputException e) {
            throw Status.INVALID_ARGUMENT.withCause(e).asRuntimeException();
          }

          return GetExpiringProfileKeyCredentialResponse.newBuilder()
              .setProfileKeyCredential(ByteString.copyFrom(profileKeyCredentialResponse.serialize()))
              .build();
        })
        .switchIfEmpty(Mono.error(Status.NOT_FOUND.withDescription("Profile version not found").asException()));
  }
}
