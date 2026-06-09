package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.CreateProfileRequest;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;

public class ProfileHelper {
  public static int MAX_PROFILE_AVATAR_SIZE_BYTES = 10 * 1024 * 1024;
  @VisibleForTesting
  public static final Duration EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION = Duration.ofDays(7);

  public static List<AccountBadge> mergeBadgeIdsWithExistingAccountBadges(
      final Clock clock,
      final Map<String, BadgeConfiguration> badgeConfigurationMap,
      final List<String> badgeIds,
      final List<AccountBadge> accountBadges) {
    LinkedHashMap<String, AccountBadge> existingBadges = new LinkedHashMap<>(accountBadges.size());
    for (final AccountBadge accountBadge : accountBadges) {
      existingBadges.putIfAbsent(accountBadge.id(), accountBadge);
    }

    LinkedHashMap<String, AccountBadge> result = new LinkedHashMap<>(accountBadges.size());
    for (final String badgeId : badgeIds) {

      // duplicate in the list, ignore it
      if (result.containsKey(badgeId)) {
        continue;
      }

      // This is for testing badges and allows them to be added to an account at any time with an expiration of 1 day
      // in the future.
      BadgeConfiguration badgeConfiguration = badgeConfigurationMap.get(badgeId);
      if (badgeConfiguration != null && badgeConfiguration.isTestBadge()) {
        result.put(badgeId, new AccountBadge(badgeId, clock.instant().plus(Duration.ofDays(1)), true));
        continue;
      }

      // reordering or making visible existing badges
      if (existingBadges.containsKey(badgeId)) {
        AccountBadge accountBadge = existingBadges.get(badgeId).withVisibility(true);
        result.put(badgeId, accountBadge);
      }
    }

    // take any remaining account badges and make them invisible
    for (final Map.Entry<String, AccountBadge> entry : existingBadges.entrySet()) {
      if (!result.containsKey(entry.getKey())) {
        AccountBadge accountBadge = entry.getValue().withVisibility(false);
        result.put(accountBadge.id(), accountBadge);
      }
    }

    return new ArrayList<>(result.values());
  }

  public static String generateAvatarObjectName() {
    final byte[] object = new byte[16];
    new SecureRandom().nextBytes(object);

    return "profiles/" + Base64.getUrlEncoder().encodeToString(object);
  }

  public static boolean isSelfProfileRequest(@Nullable final UUID requesterUuid, final ServiceIdentifier targetIdentifier) {
    return targetIdentifier.uuid().equals(requesterUuid);
  }

  public static ExpiringProfileKeyCredentialResponse getExpiringProfileKeyCredential(
      final ProfileKeyCredentialRequest request,
      final ProfileKeyCommitment commitment,
      final ServiceId.Aci accountIdentifier,
      final ServerZkProfileOperations zkProfileOperations) throws InvalidInputException, VerificationFailedException {

    final Instant expiration = Instant.now().plus(EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION).truncatedTo(ChronoUnit.DAYS);

    return zkProfileOperations.issueExpiringProfileKeyCredential(request, accountIdentifier, commitment, expiration);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static boolean isPaymentAddressUpdateForbidden(
      final Account account, final Optional<VersionedProfile> maybeProfile,
      final Optional<VersionedProfileV1> maybeV1Profile,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    final Optional<byte[]> currentPaymentAddress = maybeProfile.map(VersionedProfile::paymentAddress)
        .or(() -> maybeV1Profile.map(VersionedProfileV1::paymentAddress));
    final boolean hasDisallowedPrefix = dynamicConfigurationManager.getConfiguration().getPaymentsConfiguration()
        .getDisallowedPrefixes().stream()
        .anyMatch(prefix -> account.getNumber().startsWith(prefix));

    return hasDisallowedPrefix && currentPaymentAddress.filter(a -> a.length != 0).isEmpty();
  }

  @Nullable
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static String getAvatar(final CreateProfileRequest.AvatarChange avatarChange, final Optional<String> currentAvatar) {
    return switch (avatarChange) {
      case UNCHANGED -> currentAvatar.orElse(null);
      case CLEAR -> null;
      case UPDATE -> ProfileHelper.generateAvatarObjectName();
    };
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static Optional<String> getCurrentAvatar(Optional<VersionedProfileV1> maybeV1Profile) {
    return maybeV1Profile.map(VersionedProfileV1::avatar).filter(avatar -> avatar.startsWith("profiles/"));
  }
}
