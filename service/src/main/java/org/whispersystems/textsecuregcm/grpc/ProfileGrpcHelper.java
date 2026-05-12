/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.signal.chat.profile.Badge;
import org.signal.chat.profile.BadgeSvg;
import org.signal.chat.profile.DataEtag;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialResult;
import org.signal.chat.profile.GetUnversionedProfileResult;
import org.signal.chat.profile.GetVersionedProfileResult;
import org.signal.chat.profile.GetVersionedProfileV1Response;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;
import org.whispersystems.textsecuregcm.util.ProfileHelper;

public class ProfileGrpcHelper {
  static Optional<GetVersionedProfileResult> getVersionedProfile(final Account account,
      final ProfilesManager profilesManager,
      final byte[] requestVersion,
      final byte[] requestDataEtag,
      final byte[] requestPaymentAddressEtag) {

    final Optional<VersionedProfile> profile;
    if (account.hasCapability(DeviceCapability.PROFILES_V2)) {
      profile = profilesManager.get(account.getUuid(), requestVersion);
    } else {
      profile = Optional.empty();
    }

    final Optional<VersionedProfileV1> v1Profile;
    if (profile.isEmpty()) {
      v1Profile = profilesManager.getV1(account.getUuid(), HexFormat.of().formatHex(requestVersion));
    } else {
      v1Profile = Optional.empty();
    }

    if (profile.isEmpty() && v1Profile.isEmpty()) {
      return Optional.empty();
    }

    final GetVersionedProfileResult.Builder responseBuilder = GetVersionedProfileResult.newBuilder();

    profile.ifPresent(p -> {
      if (requestDataEtag.length > 0 && Arrays.equals(requestDataEtag, p.dataHash())) {
        responseBuilder.setDataEtagMatched(true);
      } else {
        responseBuilder.setDataEtag(DataEtag.newBuilder()
            .setData(ByteString.copyFrom(p.data()))
            .setEtagSha256(ByteString.copyFrom(p.dataHash()))
            .build());
      }
    });

    v1Profile.ifPresent(p -> {
      final GetVersionedProfileV1Response.Builder v1ResponseBuilder = GetVersionedProfileV1Response.newBuilder();

      v1ResponseBuilder
          .setName(ByteString.copyFrom(p.name()))
          .setAbout(ByteString.copyFrom(p.about()))
          .setAboutEmoji(ByteString.copyFrom(p.aboutEmoji()))
          .setPhoneNumberSharing(ByteString.copyFrom(p.phoneNumberSharing()));

      if (p.avatar() != null) {
        v1ResponseBuilder.setAvatar(p.avatar());
      }

      responseBuilder.setV1Response(v1ResponseBuilder);
    });

    // Include payment address if the version matches the latest version on Account or the latest version on Account
    // is empty
    if (account.getCurrentProfileVersion().map(v -> v.equals(HexFormat.of().formatHex(requestVersion))).orElse(true)) {
      final Optional<byte []> paymentAddress = profile.map(VersionedProfile::paymentAddress).or(() -> v1Profile.map(VersionedProfileV1::paymentAddress));

      if (paymentAddress.isPresent()) {

        final boolean paymentAddressETagMatches = requestPaymentAddressEtag.length > 0 &&
            profile.map(VersionedProfile::paymentAddressHash)
                .map(h -> Arrays.equals(requestPaymentAddressEtag, h))
                .orElse(false);

        if (paymentAddressETagMatches) {
          responseBuilder.setPaymentAddressEtagMatched(true);
        } else {
          responseBuilder.setPaymentAddressDataEtag(DataEtag.newBuilder()
              .setData(ByteString.copyFrom(paymentAddress.get()))
              .setEtagSha256(ByteString.copyFrom(
                  profile.map(VersionedProfile::paymentAddressHash).orElseGet(() -> hash(paymentAddress.get()))))
              .build());
        }
      }
    }

    return Optional.of(responseBuilder.build());
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

  static GetUnversionedProfileResult buildUnversionedProfileResult(
      final ServiceIdentifier targetIdentifier,
      final UUID requesterUuid,
      final Account targetAccount,
      final ProfileBadgeConverter profileBadgeConverter) {
    final GetUnversionedProfileResult.Builder resultBuilder = GetUnversionedProfileResult.newBuilder()
        .setIdentityKey(ByteString.copyFrom(targetAccount.getIdentityKey(targetIdentifier.identityType()).serialize()))
        .addAllCapabilities(buildAccountCapabilities(targetAccount));

    switch (targetIdentifier.identityType()) {
      case ACI -> {
        resultBuilder.setUnrestrictedUnidentifiedAccess(targetAccount.isUnrestrictedUnidentifiedAccess())
            .addAllBadges(
                buildBadges(profileBadgeConverter.convert(RequestAttributesUtil.getAvailableAcceptedLocales(),
                targetAccount.getBadges(),
                ProfileHelper.isSelfProfileRequest(requesterUuid, targetIdentifier))));

        targetAccount.getUnidentifiedAccessKey()
            .map(UnidentifiedAccessChecksum::generateFor)
            .map(ByteString::copyFrom)
            .ifPresent(resultBuilder::setUnidentifiedAccess);
      }
      case PNI -> resultBuilder.setUnrestrictedUnidentifiedAccess(false);
    }

    return resultBuilder.build();
  }

  static Optional<GetExpiringProfileKeyCredentialResult> getExpiringProfileKeyCredentialResult(
      final Account account,
      final byte[] version,
      final byte[] encodedCredentialRequest,
      final ProfilesManager profilesManager,
      final ServerZkProfileOperations zkProfileOperations) {

    final Optional<VersionedProfile> versionedProfile;
    if (account.hasCapability(DeviceCapability.PROFILES_V2)) {
      versionedProfile = profilesManager.get(account.getUuid(), version);
    } else {
      versionedProfile = Optional.empty();
    }

    return versionedProfile.map(VersionedProfile::commitment)
        .or(() -> profilesManager.getV1(account.getUuid(), HexFormat.of().formatHex(version)).map(VersionedProfileV1::commitment))
        .map(commitment -> {

          final ExpiringProfileKeyCredentialResponse profileKeyCredentialResponse;
          try {
            profileKeyCredentialResponse = ProfileHelper.getExpiringProfileKeyCredential(new ProfileKeyCredentialRequest(encodedCredentialRequest),
                new ProfileKeyCommitment(commitment), new ServiceId.Aci(account.getUuid()), zkProfileOperations);
          } catch (VerificationFailedException | InvalidInputException e) {
            throw GrpcExceptions.constraintViolation("invalid credential request");
          }

          return GetExpiringProfileKeyCredentialResult.newBuilder()
              .setProfileKeyCredential(ByteString.copyFrom(profileKeyCredentialResponse.serialize()))
              .build();
        });
  }

  /**
   * Computes the SHA-256 hash of the given data.
   * <p>
   * Only needed during the v1 -> v2 migration. See also: VersionedProfile#hash(byte[])
   */
  @VisibleForTesting
  public static byte[] hash(final byte[] data) {
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    return sha256.digest(data);
  }
}
