package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.signal.chat.profile.Badge;
import org.signal.chat.profile.BadgeSvg;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.UserCapabilities;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.ProfileHelper;

public class ProfileGrpcHelper {
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
  static UserCapabilities buildUserCapabilities(final org.whispersystems.textsecuregcm.entities.UserCapabilities capabilities) {
    return UserCapabilities.newBuilder()
        .setGv1Migration(capabilities.gv1Migration())
        .setSenderKey(capabilities.senderKey())
        .setAnnouncementGroup(capabilities.announcementGroup())
        .setChangeNumber(capabilities.changeNumber())
        .setStories(capabilities.stories())
        .setGiftBadges(capabilities.giftBadges())
        .setPaymentActivation(capabilities.paymentActivation())
        .setPni(capabilities.pni())
        .build();
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
        .setCapabilities(buildUserCapabilities(org.whispersystems.textsecuregcm.entities.UserCapabilities.createForAccount(targetAccount)));

    switch (targetIdentifier.identityType()) {
      case ACI -> {
        responseBuilder.setUnrestrictedUnidentifiedAccess(targetAccount.isUnrestrictedUnidentifiedAccess())
            .addAllBadges(buildBadges(profileBadgeConverter.convert(
                AcceptLanguageUtil.localeFromGrpcContext(),
                targetAccount.getBadges(),
                ProfileHelper.isSelfProfileRequest(requesterUuid, (AciServiceIdentifier) targetIdentifier))));

        targetAccount.getUnidentifiedAccessKey()
            .map(UnidentifiedAccessChecksum::generateFor)
            .map(ByteString::copyFrom)
            .ifPresent(responseBuilder::setUnidentifiedAccess);
      }
      case PNI -> responseBuilder.setUnrestrictedUnidentifiedAccess(false);
    }

    return responseBuilder.build();
  }
}
