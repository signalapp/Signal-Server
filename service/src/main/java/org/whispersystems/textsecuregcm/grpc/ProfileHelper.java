package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.signal.chat.profile.ProfileAvatarUploadAttributes;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.util.Pair;

public class ProfileHelper {
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
    byte[] object = new byte[16];
    new SecureRandom().nextBytes(object);

    return "profiles/" + Base64.getUrlEncoder().encodeToString(object);
  }

  public static org.signal.chat.profile.ProfileAvatarUploadAttributes generateAvatarUploadFormGrpc(
      final PostPolicyGenerator policyGenerator,
      final PolicySigner policySigner,
      final String objectName) {
    final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    final Pair<String, String> policy = policyGenerator.createFor(now, objectName, 10 * 1024 * 1024);
    final String signature = policySigner.getSignature(now, policy.second());

    return org.signal.chat.profile.ProfileAvatarUploadAttributes.newBuilder()
        .setPath(objectName)
        .setCredential(policy.first())
        .setAcl("private")
        .setAlgorithm("AWS4-HMAC-SHA256")
        .setDate(now.format(PostPolicyGenerator.AWS_DATE_TIME))
        .setPolicy(policy.second())
        .setSignature(ByteString.copyFrom(signature.getBytes()))
        .build();
  }

  public static org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes generateAvatarUploadForm(
      final PostPolicyGenerator policyGenerator,
      final PolicySigner policySigner,
      final String objectName) {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    Pair<String, String> policy = policyGenerator.createFor(now, objectName, 10 * 1024 * 1024);
    String signature = policySigner.getSignature(now, policy.second());

    return new org.whispersystems.textsecuregcm.entities.ProfileAvatarUploadAttributes(objectName, policy.first(), "private", "AWS4-HMAC-SHA256",
        now.format(PostPolicyGenerator.AWS_DATE_TIME), policy.second(), signature);

  }
}
