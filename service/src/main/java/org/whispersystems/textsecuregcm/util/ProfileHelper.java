package org.whispersystems.textsecuregcm.util;

import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import javax.annotation.Nullable;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ProfileHelper {
  public static int MAX_PROFILE_AVATAR_SIZE_BYTES = 10 * 1024 * 1024;
  public static List<AccountBadge> mergeBadgeIdsWithExistingAccountBadges(
      final Clock clock,
      final Map<String, BadgeConfiguration> badgeConfigurationMap,
      final List<String> badgeIds,
      final List<AccountBadge> accountBadges) {
    LinkedHashMap<String, AccountBadge> existingBadges = new LinkedHashMap<>(accountBadges.size());
    for (final AccountBadge accountBadge : accountBadges) {
      existingBadges.putIfAbsent(accountBadge.getId(), accountBadge);
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
        result.put(accountBadge.getId(), accountBadge);
      }
    }

    return new ArrayList<>(result.values());
  }

  public static String generateAvatarObjectName() {
    final byte[] object = new byte[16];
    new SecureRandom().nextBytes(object);

    return "profiles/" + Base64.getUrlEncoder().encodeToString(object);
  }

  public static boolean isSelfProfileRequest(@Nullable final UUID requesterUuid, final AciServiceIdentifier targetIdentifier) {
    return targetIdentifier.uuid().equals(requesterUuid);
  }
}
