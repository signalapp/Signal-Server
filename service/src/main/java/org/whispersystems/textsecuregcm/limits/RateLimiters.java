/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;


import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RateLimiters extends BaseRateLimiters<RateLimiters.For> {

  public enum For implements RateLimiterDescriptor {
    BACKUP_AUTH_CHECK("backupAuthCheck", false, new RateLimiterConfig(100, Duration.ofMinutes(15))),
    PIN("pin", false, new RateLimiterConfig(10, Duration.ofDays(1))),
    ATTACHMENT("attachmentCreate", false, new RateLimiterConfig(50, Duration.ofMillis(1200))),
    BACKUP_ATTACHMENT("backupAttachmentCreate", true, new RateLimiterConfig(10_000, Duration.ofSeconds(1))),
    PRE_KEYS("prekeys", false, new RateLimiterConfig(6, Duration.ofMinutes(10))),
    MESSAGES("messages", false, new RateLimiterConfig(60, Duration.ofSeconds(1))),
    STORIES("stories", false, new RateLimiterConfig(5_000, Duration.ofSeconds(8))),
    ALLOCATE_DEVICE("allocateDevice", false, new RateLimiterConfig(6, Duration.ofMinutes(2))),
    VERIFY_DEVICE("verifyDevice", false, new RateLimiterConfig(6, Duration.ofMinutes(2))),
    TURN("turnAllocate", false, new RateLimiterConfig(60, Duration.ofSeconds(1))),
    PROFILE("profile", false, new RateLimiterConfig(4320, Duration.ofSeconds(20))),
    STICKER_PACK("stickerPack", false, new RateLimiterConfig(50, Duration.ofMinutes(72))),
    USERNAME_LOOKUP("usernameLookup", false, new RateLimiterConfig(100, Duration.ofMinutes(15))),
    USERNAME_SET("usernameSet", false, new RateLimiterConfig(100, Duration.ofMinutes(15))),
    USERNAME_RESERVE("usernameReserve", false, new RateLimiterConfig(100, Duration.ofMinutes(15))),
    USERNAME_LINK_OPERATION("usernameLinkOperation", false, new RateLimiterConfig(10, Duration.ofMinutes(1))),
    USERNAME_LINK_LOOKUP_PER_IP("usernameLinkLookupPerIp", false, new RateLimiterConfig(100, Duration.ofSeconds(15))),
    CHECK_ACCOUNT_EXISTENCE("checkAccountExistence", false, new RateLimiterConfig(1000, Duration.ofSeconds(4))),
    REGISTRATION("registration", false, new RateLimiterConfig(6, Duration.ofSeconds(30))),
    VERIFICATION_PUSH_CHALLENGE("verificationPushChallenge", false, new RateLimiterConfig(5, Duration.ofSeconds(30))),
    VERIFICATION_CAPTCHA("verificationCaptcha", false, new RateLimiterConfig(10, Duration.ofSeconds(30))),
    RATE_LIMIT_RESET("rateLimitReset", true, new RateLimiterConfig(2, Duration.ofHours(12))),
    CAPTCHA_CHALLENGE_ATTEMPT("captchaChallengeAttempt", true, new RateLimiterConfig(10, Duration.ofMinutes(144))),
    CAPTCHA_CHALLENGE_SUCCESS("captchaChallengeSuccess", true, new RateLimiterConfig(2, Duration.ofHours(12))),
    SET_BACKUP_ID("setBackupId", true, new RateLimiterConfig(10, Duration.ofHours(1))),
    SET_PAID_MEDIA_BACKUP_ID("setPaidMediaBackupId", true, new RateLimiterConfig(5, Duration.ofDays(7))),
    PUSH_CHALLENGE_ATTEMPT("pushChallengeAttempt", true, new RateLimiterConfig(10, Duration.ofMinutes(144))),
    PUSH_CHALLENGE_SUCCESS("pushChallengeSuccess", true, new RateLimiterConfig(2, Duration.ofHours(12))),
    GET_CALLING_RELAYS("getCallingRelays", false, new RateLimiterConfig(100, Duration.ofMinutes(10))),
    CREATE_CALL_LINK("createCallLink", false, new RateLimiterConfig(100, Duration.ofMinutes(15))),
    INBOUND_MESSAGE_BYTES("inboundMessageBytes", true, new RateLimiterConfig(128 * 1024 * 1024, Duration.ofNanos(500_000))),
    EXTERNAL_SERVICE_CREDENTIALS("externalServiceCredentials", true, new RateLimiterConfig(100, Duration.ofMinutes(15))),
    KEY_TRANSPARENCY_DISTINGUISHED_PER_IP("keyTransparencyDistinguished", true,
        new RateLimiterConfig(100, Duration.ofSeconds(15))),
    KEY_TRANSPARENCY_SEARCH_PER_IP("keyTransparencySearch", true, new RateLimiterConfig(100, Duration.ofSeconds(15))),
    KEY_TRANSPARENCY_MONITOR_PER_IP("keyTransparencyMonitor", true, new RateLimiterConfig(100, Duration.ofSeconds(15))),
    WAIT_FOR_LINKED_DEVICE("waitForLinkedDevice", true, new RateLimiterConfig(10, Duration.ofSeconds(30))),
    UPLOAD_TRANSFER_ARCHIVE("uploadTransferArchive", true, new RateLimiterConfig(10, Duration.ofMinutes(1))),
    WAIT_FOR_TRANSFER_ARCHIVE("waitForTransferArchive", true, new RateLimiterConfig(10, Duration.ofSeconds(30))),
    RECORD_DEVICE_TRANSFER_REQUEST("recordDeviceTransferRequest", true, new RateLimiterConfig(10, Duration.ofMillis(100))),
    WAIT_FOR_DEVICE_TRANSFER_REQUEST("waitForDeviceTransferRequest", true, new RateLimiterConfig(10, Duration.ofMillis(100))),
    DEVICE_CHECK_CHALLENGE("deviceCheckChallenge", true, new RateLimiterConfig(10, Duration.ofMinutes(1))),
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
      final FaultTolerantRedisClusterClient cacheCluster) {
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
      final FaultTolerantRedisClusterClient cacheCluster,
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

  public RateLimiter getUsernameLookupLimiter() {
    return forDescriptor(For.USERNAME_LOOKUP);
  }

  public RateLimiter getUsernameLinkLookupLimiter() {
    return forDescriptor(For.USERNAME_LINK_LOOKUP_PER_IP);
  }

  public RateLimiter getUsernameLinkOperationLimiter() {
    return forDescriptor(For.USERNAME_LINK_OPERATION);
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

  public RateLimiter getCaptchaChallengeAttemptLimiter() {
    return forDescriptor(For.CAPTCHA_CHALLENGE_ATTEMPT);
  }

  public RateLimiter getCaptchaChallengeSuccessLimiter() {
    return forDescriptor(For.CAPTCHA_CHALLENGE_SUCCESS);
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

  public RateLimiter getCallEndpointLimiter() {
    return forDescriptor(For.GET_CALLING_RELAYS);
  }

  public RateLimiter getInboundMessageBytes() {
    return forDescriptor(For.INBOUND_MESSAGE_BYTES);
  }

  public RateLimiter getStoriesLimiter() {
    return forDescriptor(For.STORIES);
  }

  public RateLimiter getWaitForLinkedDeviceLimiter() {
    return forDescriptor(For.WAIT_FOR_LINKED_DEVICE);
  }

  public RateLimiter getUploadTransferArchiveLimiter() {
    return forDescriptor(For.UPLOAD_TRANSFER_ARCHIVE);
  }

  public RateLimiter getWaitForTransferArchiveLimiter() {
    return forDescriptor(For.WAIT_FOR_TRANSFER_ARCHIVE);
  }
}
