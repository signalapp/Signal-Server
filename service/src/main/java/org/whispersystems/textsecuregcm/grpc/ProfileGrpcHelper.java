/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import org.bouncycastle.crypto.digests.TupleHash;
import org.signal.chat.common.S3UploadForm;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialResult;
import org.signal.chat.profile.ProfileResult;
import org.signal.chat.profile.ProfileResultOrBuilder;
import org.signal.chat.profile.LegacyProfileResult;
import org.signal.chat.profile.AccountInfo;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;
import org.whispersystems.textsecuregcm.util.ProfileHelper;

public class ProfileGrpcHelper {

  private static final byte[] ETAG_DOMAIN = "ProfileETag/v1".getBytes(StandardCharsets.UTF_8);
  private static final int ETAG_LENGTH = 10;
  private static final S3UploadForm PROTOTYPE_AVATAR_UPLOAD_FORM = S3UploadForm.newBuilder()
      .setAcl(PostPolicyGenerator.ACL)
      .setAlgorithm(PostPolicyGenerator.ALGORITHM)
      .build();

  static Optional<LegacyProfileResult> getProfileV1(final Account account,
      final ProfilesManager profilesManager,
      final ProfileBadgeConverter profileBadgeConverter,
      final byte[] requestVersion) {
    final Optional<VersionedProfileV1> maybeV1Profile =
        profilesManager.getV1(account.getUuid(), HexFormat.of().formatHex(requestVersion));
    if (maybeV1Profile.isEmpty()) {
      return Optional.empty();
    }
    final VersionedProfileV1 v1Profile = maybeV1Profile.get();

    final LegacyProfileResult.Builder builder = LegacyProfileResult.newBuilder()
        .setAccountInfo(buildAccountInfo(profileBadgeConverter, account))
        .setName(ByteString.copyFrom(v1Profile.name()))
        .setAbout(ByteString.copyFrom(v1Profile.about()))
        .setAboutEmoji(ByteString.copyFrom(v1Profile.aboutEmoji()))
        .setPhoneNumberSharing(ByteString.copyFrom(v1Profile.phoneNumberSharing()));

    if (v1Profile.avatar() != null) {
      builder.setAvatar(v1Profile.avatar());
    }

    // If present, include the payment address if the version matches the latest version on Account or the latest
    // version on Account is empty
    if (v1Profile.paymentAddress() != null && account.getCurrentProfileVersion().map(v -> Arrays.equals(v, requestVersion)).orElse(true)) {
      builder.setPaymentAddress(ByteString.copyFrom(v1Profile.paymentAddress()));
    }

    return Optional.of(builder.build());
  }

  static Optional<ProfileResult> getProfile(final Account account,
      final ProfilesManager profilesManager,
      final ProfileBadgeConverter profileBadgeConverter,
      final byte[] requestVersion) {

    final Optional<VersionedProfile> maybeProfile = account.hasCapability(DeviceCapability.PROFILES_V2)
        ? profilesManager.get(account.getUuid(), requestVersion)
        : Optional.empty();
    if (maybeProfile.isEmpty()) {
      return Optional.empty();
    }
    final VersionedProfile profile = maybeProfile.get();

    final ProfileResult.Builder builder = ProfileResult.newBuilder()
        .setData(ByteString.copyFrom(profile.data()))
        .setAccountInfo(buildAccountInfo(profileBadgeConverter, account));

    // If present, include the payment address if the version matches the latest version on Account or the latest
    // version on Account is empty
    if (profile.paymentAddress() != null && account.getCurrentProfileVersion().map(v -> Arrays.equals(v, requestVersion)).orElse(true)) {
      builder.setPaymentAddress(ByteString.copyFrom(profile.paymentAddress()));
    }

    builder.setEtag(ByteString.copyFrom(etag(builder)));
    return Optional.of(builder.build());
  }

  private static AccountInfo buildAccountInfo(final ProfileBadgeConverter profileBadgeConverter,
      final Account account) {
    final AccountInfo.Builder accountInfoBuilder = AccountInfo.newBuilder()
        .addAllBadgeIds(profileBadgeConverter.visibleBadgeIds(account.getBadges()))
        .setIdentityKey(ByteString.copyFrom(account.getIdentityKey(IdentityType.ACI).serialize()))
        .setUnrestrictedUnidentifiedAccess(account.isUnrestrictedUnidentifiedAccess());

    account.getUnidentifiedAccessKey()
        .map(UnidentifiedAccessChecksum::generateFor)
        .map(ByteString::copyFrom)
        .ifPresent(accountInfoBuilder::setUnidentifiedAccessKeyFingerprint);

    return accountInfoBuilder.build();
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
        .or(() -> profilesManager.getV1(account.getUuid(), HexFormat.of().formatHex(version))
            .map(VersionedProfileV1::commitment))
        .map(commitment -> {

          final ExpiringProfileKeyCredentialResponse profileKeyCredentialResponse;
          try {
            profileKeyCredentialResponse = ProfileHelper.getExpiringProfileKeyCredential(
                new ProfileKeyCredentialRequest(encodedCredentialRequest),
                new ProfileKeyCommitment(commitment), new ServiceId.Aci(account.getUuid()), zkProfileOperations);
          } catch (VerificationFailedException | InvalidInputException e) {
            throw GrpcExceptions.invalidArguments("invalid credential request");
          }

          return GetExpiringProfileKeyCredentialResult.newBuilder()
              .setProfileKeyCredential(ByteString.copyFrom(profileKeyCredentialResponse.serialize()))
              .build();
        });
  }

  public static S3UploadForm generateAvatarUploadForm(final String objectName, final int uploadLength,
      final PostPolicyGenerator policyGenerator, final Clock clock) {
    final PostPolicyGenerator.SignedPostPolicy policy =
        policyGenerator.createFor(objectName, uploadLength, clock.instant());

    return PROTOTYPE_AVATAR_UPLOAD_FORM.toBuilder()
        .setKey(objectName)
        .setCredential(policy.credential())
        .setDate(policy.formattedTimestamp())
        .setPolicy(policy.encodedPolicy())
        .setSignature(policy.signature())
        .build();
  }

  /// Compute the etag of a [ProfileResult]
  ///
  /// @implNote This uses the NIST-SP 800-185 TupleHash to provide a cryptographic hash over variable length fields.
  /// Clients must be able to reproduce the same etag from the data, so hash must be stable.
  @VisibleForTesting
  static byte[] etag(final ProfileResultOrBuilder profile) {
    final TupleHash h = new TupleHash(256, ETAG_DOMAIN, ETAG_LENGTH * Byte.SIZE);
    final AccountInfo accountInfo = profile.getAccountInfo();

    addByteArray(h, profile.getData().toByteArray());
    addByteArray(h, profile.getPaymentAddress().toByteArray());
    addByteArray(h, accountInfo.getIdentityKey().toByteArray());
    addByteArray(h, accountInfo.getUnidentifiedAccessKeyFingerprint().toByteArray());
    h.update(accountInfo.getUnrestrictedUnidentifiedAccess() ? (byte) 1 : (byte) 0);

    addInt(h, accountInfo.getBadgeIdsCount());
    accountInfo.getBadgeIdsList().asByteStringList().stream()
        .map(ByteString::toByteArray)
        .sorted(Arrays::compareUnsigned)
        .forEach(badgeId -> addByteArray(h, badgeId));

    final byte[] out = new byte[ETAG_LENGTH];
    h.doFinal(out, 0);
    return out;
  }

  private static void addByteArray(final TupleHash hash, final byte[] bytes) {
    hash.update(bytes, 0, bytes.length);
  }

  private static void addInt(final TupleHash hash, final int v) {
    addByteArray(hash, ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(v).array());
  }
}
