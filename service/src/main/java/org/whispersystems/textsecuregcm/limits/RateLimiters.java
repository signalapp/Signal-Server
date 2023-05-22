/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;


import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RateLimiters extends BaseRateLimiters<RateLimiters.For> {

  public enum For implements RateLimiterDescriptor {
    BACKUP_AUTH_CHECK("backupAuthCheck", false, new RateLimiterConfig(100, OptionalDouble.of(100 / (24.0 * 60.0)), Optional.empty())),

    SMS_DESTINATION("smsDestination", false, new RateLimiterConfig(2, OptionalDouble.of(2), Optional.empty())),

    VOICE_DESTINATION("voxDestination", false, new RateLimiterConfig(2, OptionalDouble.of(1.0 / 2.0), Optional.empty())),

    VOICE_DESTINATION_DAILY("voxDestinationDaily", false, new RateLimiterConfig(10, OptionalDouble.of(10.0 / (24.0 * 60.0)),
        Optional.empty())),

    SMS_VOICE_IP("smsVoiceIp", false, new RateLimiterConfig(1000, OptionalDouble.of(1000), Optional.empty())),

    SMS_VOICE_PREFIX("smsVoicePrefix", false, new RateLimiterConfig(1000, OptionalDouble.of(1000), Optional.empty())),

    VERIFY("verify", false, new RateLimiterConfig(6, OptionalDouble.of(2), Optional.empty())),

    PIN("pin", false, new RateLimiterConfig(10, OptionalDouble.of(1 / (24.0 * 60.0)), Optional.empty())),

    ATTACHMENT("attachmentCreate", false, new RateLimiterConfig(50, OptionalDouble.of(50), Optional.empty())),

    PRE_KEYS("prekeys", false, new RateLimiterConfig(6, OptionalDouble.of(1.0 / 10.0), Optional.empty())),

    MESSAGES("messages", false, new RateLimiterConfig(60, OptionalDouble.of(60), Optional.empty())),

    ALLOCATE_DEVICE("allocateDevice", false, new RateLimiterConfig(2, OptionalDouble.of(1.0 / 2.0), Optional.empty())),

    VERIFY_DEVICE("verifyDevice", false, new RateLimiterConfig(6, OptionalDouble.of(1.0 / 10.0), Optional.empty())),

    TURN("turnAllocate", false, new RateLimiterConfig(60, OptionalDouble.of(60), Optional.empty())),

    PROFILE("profile", false, new RateLimiterConfig(4320, OptionalDouble.of(3), Optional.empty())),

    STICKER_PACK("stickerPack", false, new RateLimiterConfig(50, OptionalDouble.of(20 / (24.0 * 60.0)), Optional.empty())),

    ART_PACK("artPack", false, new RateLimiterConfig(50, OptionalDouble.of(20 / (24.0 * 60.0)), Optional.empty())),

    USERNAME_LOOKUP("usernameLookup", false, new RateLimiterConfig(100, OptionalDouble.of(100 / (24.0 * 60.0)), Optional.empty())),

    USERNAME_SET("usernameSet", false, new RateLimiterConfig(100, OptionalDouble.of(100 / (24.0 * 60.0)), Optional.empty())),

    USERNAME_RESERVE("usernameReserve", false, new RateLimiterConfig(100, OptionalDouble.of(100 / (24.0 * 60.0)), Optional.empty())),

    CHECK_ACCOUNT_EXISTENCE("checkAccountExistence", false, new RateLimiterConfig(1_000, OptionalDouble.of(1_000 / 60.0), Optional.empty())),

    REGISTRATION("registration", false, new RateLimiterConfig(6, OptionalDouble.of(2), Optional.empty())),

    VERIFICATION_PUSH_CHALLENGE("verificationPushChallenge", false, new RateLimiterConfig(5, OptionalDouble.of(2), Optional.empty())),

    VERIFICATION_CAPTCHA("verificationCaptcha", false, new RateLimiterConfig(10, OptionalDouble.of(2), Optional.empty())),

    RATE_LIMIT_RESET("rateLimitReset", true, new RateLimiterConfig(2, OptionalDouble.of(2.0 / (60 * 24)), Optional.empty())),

    RECAPTCHA_CHALLENGE_ATTEMPT("recaptchaChallengeAttempt", true, new RateLimiterConfig(10, OptionalDouble.of(10.0 / (60 * 24)),
        Optional.empty())),

    RECAPTCHA_CHALLENGE_SUCCESS("recaptchaChallengeSuccess", true, new RateLimiterConfig(2, OptionalDouble.of(2.0 / (60 * 24)),
        Optional.empty())),

    PUSH_CHALLENGE_ATTEMPT("pushChallengeAttempt", true, new RateLimiterConfig(10, OptionalDouble.of(10.0 / (60 * 24)), Optional.empty())),

    PUSH_CHALLENGE_SUCCESS("pushChallengeSuccess", true, new RateLimiterConfig(2, OptionalDouble.of(2.0 / (60 * 24)), Optional.empty())),

    CREATE_CALL_LINK("createCallLink", false, new RateLimiterConfig(100, OptionalDouble.of(100.0 / (60 * 24)), Optional.empty()));
    ;

    private final String id;

    private final boolean dynamic;

    private final RateLimiterConfig defaultConfig;

    For(final String id, final boolean dynamic, final RateLimiterConfig defaultConfig) {
      this.id = id;
      this.dynamic = dynamic;
      this.defaultConfig = defaultConfig;
    }

    public String id() {
      return id;
    }

    @Override
    public boolean isDynamic() {
      return dynamic;
    }

    public RateLimiterConfig defaultConfig() {
      return defaultConfig;
    }
  }

  public static RateLimiters createAndValidate(
      final Map<String, RateLimiterConfig> configs,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final FaultTolerantRedisCluster cacheCluster) {
    final RateLimiters rateLimiters = new RateLimiters(
        configs, dynamicConfigurationManager, defaultScript(cacheCluster), cacheCluster, Clock.systemUTC());
    rateLimiters.validateValuesAndConfigs();
    return rateLimiters;
  }

  @VisibleForTesting
  RateLimiters(
      final Map<String, RateLimiterConfig> configs,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ClusterLuaScript validateScript,
      final FaultTolerantRedisCluster cacheCluster,
      final Clock clock) {
    super(For.values(), configs, dynamicConfigurationManager, validateScript, cacheCluster, clock);
  }

  public RateLimiter getAllocateDeviceLimiter() {
    return forDescriptor(For.ALLOCATE_DEVICE);
  }

  public RateLimiter getVerifyDeviceLimiter() {
    return forDescriptor(For.VERIFY_DEVICE);
  }

  public RateLimiter getMessagesLimiter() {
    return forDescriptor(For.MESSAGES);
  }

  public RateLimiter getPreKeysLimiter() {
    return forDescriptor(For.PRE_KEYS);
  }

  public RateLimiter getAttachmentLimiter() {
    return forDescriptor(For.ATTACHMENT);
  }

  public RateLimiter getSmsDestinationLimiter() {
    return forDescriptor(For.SMS_DESTINATION);
  }

  public RateLimiter getSmsVoiceIpLimiter() {
    return forDescriptor(For.SMS_VOICE_IP);
  }

  public RateLimiter getSmsVoicePrefixLimiter() {
    return forDescriptor(For.SMS_VOICE_PREFIX);
  }

  public RateLimiter getVoiceDestinationLimiter() {
    return forDescriptor(For.VOICE_DESTINATION);
  }

  public RateLimiter getVoiceDestinationDailyLimiter() {
    return forDescriptor(For.VOICE_DESTINATION_DAILY);
  }

  public RateLimiter getVerifyLimiter() {
    return forDescriptor(For.VERIFY);
  }

  public RateLimiter getPinLimiter() {
    return forDescriptor(For.PIN);
  }

  public RateLimiter getTurnLimiter() {
    return forDescriptor(For.TURN);
  }

  public RateLimiter getProfileLimiter() {
    return forDescriptor(For.PROFILE);
  }

  public RateLimiter getStickerPackLimiter() {
    return forDescriptor(For.STICKER_PACK);
  }

  public RateLimiter getArtPackLimiter() {
    return forDescriptor(For.ART_PACK);
  }

  public RateLimiter getUsernameLookupLimiter() {
    return forDescriptor(For.USERNAME_LOOKUP);
  }

  public RateLimiter getUsernameSetLimiter() {
    return forDescriptor(For.USERNAME_SET);
  }

  public RateLimiter getUsernameReserveLimiter() {
    return forDescriptor(For.USERNAME_RESERVE);
  }

  public RateLimiter getCheckAccountExistenceLimiter() {
    return forDescriptor(For.CHECK_ACCOUNT_EXISTENCE);
  }

  public RateLimiter getRegistrationLimiter() {
    return forDescriptor(For.REGISTRATION);
  }

  public RateLimiter getRateLimitResetLimiter() {
    return forDescriptor(For.RATE_LIMIT_RESET);
  }

  public RateLimiter getRecaptchaChallengeAttemptLimiter() {
    return forDescriptor(For.RECAPTCHA_CHALLENGE_ATTEMPT);
  }

  public RateLimiter getRecaptchaChallengeSuccessLimiter() {
    return forDescriptor(For.RECAPTCHA_CHALLENGE_SUCCESS);
  }

  public RateLimiter getPushChallengeAttemptLimiter() {
    return forDescriptor(For.PUSH_CHALLENGE_ATTEMPT);
  }

  public RateLimiter getPushChallengeSuccessLimiter() {
    return forDescriptor(For.PUSH_CHALLENGE_SUCCESS);
  }

  public RateLimiter getVerificationPushChallengeLimiter() {
    return forDescriptor(For.VERIFICATION_PUSH_CHALLENGE);
  }

  public RateLimiter getVerificationCaptchaLimiter() {
    return forDescriptor(For.VERIFICATION_CAPTCHA);
  }

  public RateLimiter getCreateCallLinkLimiter() {
    return forDescriptor(For.CREATE_CALL_LINK);
  }
}
