/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Optional;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.ApnMessage.Type;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.GcmMessage;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PushChallengeDynamoDb;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public class PushChallengeManager {
  private final APNSender apnSender;
  private final GCMSender gcmSender;

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

  public PushChallengeManager(final APNSender apnSender, final GCMSender gcmSender,
      final PushChallengeDynamoDb pushChallengeDynamoDb) {

    this.apnSender = apnSender;
    this.gcmSender = gcmSender;
    this.pushChallengeDynamoDb = pushChallengeDynamoDb;
  }

  public void sendChallenge(final Account account) throws NotPushRegisteredException {
    final Device masterDevice = account.getMasterDevice().orElseThrow(NotPushRegisteredException::new);

    if (StringUtils.isAllBlank(masterDevice.getGcmId(), masterDevice.getApnId())) {
      throw new NotPushRegisteredException();
    }

    final byte[] token = new byte[CHALLENGE_TOKEN_LENGTH];
    random.nextBytes(token);

    final boolean sent;
    final String platform;

    if (pushChallengeDynamoDb.add(account.getUuid(), token, CHALLENGE_TTL)) {
      final String tokenHex = Hex.encodeHexString(token);
      sent = true;

      if (StringUtils.isNotBlank(masterDevice.getGcmId())) {
        gcmSender.sendMessage(new GcmMessage(masterDevice.getGcmId(), account.getUuid(), 0, GcmMessage.Type.RATE_LIMIT_CHALLENGE, Optional.of(tokenHex)));
        platform = ClientPlatform.ANDROID.name().toLowerCase();
      } else if (StringUtils.isNotBlank(masterDevice.getApnId())) {
        apnSender.sendMessage(new ApnMessage(masterDevice.getApnId(), account.getUuid(), 0, false, Type.RATE_LIMIT_CHALLENGE, Optional.of(tokenHex)));
        platform = ClientPlatform.IOS.name().toLowerCase();
      } else {
        throw new AssertionError();
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
      success = pushChallengeDynamoDb.remove(account.getUuid(), Hex.decodeHex(challengeTokenHex));
    } catch (final DecoderException ignored) {
    }

    final String platform = account.getMasterDevice().map(masterDevice -> {
      if (StringUtils.isNotBlank(masterDevice.getGcmId())) {
        return ClientPlatform.IOS.name().toLowerCase();
      } else if (StringUtils.isNotBlank(masterDevice.getApnId())) {
        return ClientPlatform.ANDROID.name().toLowerCase();
      } else {
        return "unknown";
      }
    }).orElse("unknown");


    Metrics.counter(CHALLENGE_ANSWERED_COUNTER_NAME,
        PLATFORM_TAG_NAME, platform,
        SOURCE_COUNTRY_TAG_NAME, Util.getCountryCode(account.getNumber()),
        SUCCESS_TAG_NAME, String.valueOf(success)).increment();

    return success;
  }
}
