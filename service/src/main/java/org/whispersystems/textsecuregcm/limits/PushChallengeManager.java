/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HexFormat;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PushChallengeDynamoDb;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public class PushChallengeManager {
  private final PushNotificationManager pushNotificationManager;
  private final PushChallengeDynamoDb pushChallengeDynamoDb;

  private final SecureRandom random = new SecureRandom();

  private static final int CHALLENGE_TOKEN_LENGTH = 16;
  private static final Duration CHALLENGE_TTL = Duration.ofMinutes(5);

  private static final String CHALLENGE_REQUESTED_COUNTER_NAME = name(PushChallengeManager.class, "requested");
  private static final String CHALLENGE_ANSWERED_COUNTER_NAME = name(PushChallengeManager.class, "answered");

  private static final String PLATFORM_TAG_NAME = "platform";
  private static final String SENT_TAG_NAME = "sent";
  private static final String SUCCESS_TAG_NAME = "success";
  private static final String SOURCE_COUNTRY_TAG_NAME = "sourceCountry";

  public PushChallengeManager(final PushNotificationManager pushNotificationManager,
      final PushChallengeDynamoDb pushChallengeDynamoDb) {

    this.pushNotificationManager = pushNotificationManager;
    this.pushChallengeDynamoDb = pushChallengeDynamoDb;
  }

  public void sendChallenge(final Account account) throws NotPushRegisteredException {
    final Device primaryDevice = account.getPrimaryDevice();

    final byte[] token = new byte[CHALLENGE_TOKEN_LENGTH];
    random.nextBytes(token);

    final boolean sent;
    final String platform;

    if (pushChallengeDynamoDb.add(account.getUuid(), token, CHALLENGE_TTL)) {
      pushNotificationManager.sendRateLimitChallengeNotification(account, HexFormat.of().formatHex(token));

      sent = true;

      if (StringUtils.isNotBlank(primaryDevice.getGcmId())) {
        platform = ClientPlatform.ANDROID.name().toLowerCase();
      } else if (StringUtils.isNotBlank(primaryDevice.getApnId())) {
        platform = ClientPlatform.IOS.name().toLowerCase();
      } else {
        // This should never happen; if the account has neither an APN nor FCM token, sending the challenge will result
        // in a `NotPushRegisteredException`
        platform = "unrecognized";
      }
    } else {
      sent = false;
      platform = "unrecognized";
    }

    Metrics.counter(CHALLENGE_REQUESTED_COUNTER_NAME,
        PLATFORM_TAG_NAME, platform,
        SOURCE_COUNTRY_TAG_NAME, Util.getCountryCode(account.getNumber()),
        SENT_TAG_NAME, String.valueOf(sent)).increment();
  }

  public boolean answerChallenge(final Account account, final String challengeTokenHex) {
    boolean success = false;

    try {
      success = pushChallengeDynamoDb.remove(account.getUuid(), HexFormat.of().parseHex(challengeTokenHex));
    } catch (final IllegalArgumentException ignored) {
    }

    final String platform;

    if (StringUtils.isNotBlank(account.getPrimaryDevice().getGcmId())) {
      platform = ClientPlatform.ANDROID.name().toLowerCase();
    } else if (StringUtils.isNotBlank(account.getPrimaryDevice().getApnId())) {
      platform = ClientPlatform.IOS.name().toLowerCase();
    } else {
      platform = "unknown";
    }

    Metrics.counter(CHALLENGE_ANSWERED_COUNTER_NAME,
        PLATFORM_TAG_NAME, platform,
        SOURCE_COUNTRY_TAG_NAME, Util.getCountryCode(account.getNumber()),
        SUCCESS_TAG_NAME, String.valueOf(success)).increment();

    return success;
  }
}
