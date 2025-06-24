/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class RateLimiters extends BaseRateLimiters<RateLimiters.For> {

  public enum For implements RateLimiterDescriptor {
    BACKUP_AUTH_CHECK("backupAuthCheck", new RateLimiterConfig(100, Duration.ofMinutes(15), true)),
    PIN("pin", new RateLimiterConfig(10, Duration.ofDays(1), false)),
    ATTACHMENT("attachmentCreate", new RateLimiterConfig(50, Duration.ofMillis(1200), true)),
    BACKUP_ATTACHMENT("backupAttachmentCreate", new RateLimiterConfig(10_000, Duration.ofSeconds(1), true)),
    PRE_KEYS("prekeys", new RateLimiterConfig(6, Duration.ofMinutes(10), false)),
    MESSAGES("messages", new RateLimiterConfig(60, Duration.ofSeconds(1), true)),
    STORIES("stories", new RateLimiterConfig(5_000, Duration.ofSeconds(8), true)),
    ALLOCATE_DEVICE("allocateDevice", new RateLimiterConfig(6, Duration.ofMinutes(2), false)),
    VERIFY_DEVICE("verifyDevice", new RateLimiterConfig(6, Duration.ofMinutes(2), false)),
    PROFILE("profile", new RateLimiterConfig(4320, Duration.ofSeconds(20), true)),
    STICKER_PACK("stickerPack", new RateLimiterConfig(50, Duration.ofMinutes(72), false)),
    USERNAME_LOOKUP("usernameLookup", new RateLimiterConfig(100, Duration.ofMinutes(15), true)),
    USERNAME_SET("usernameSet", new RateLimiterConfig(100, Duration.ofMinutes(15), false)),
    USERNAME_RESERVE("usernameReserve", new RateLimiterConfig(100, Duration.ofMinutes(15), false)),
    USERNAME_LINK_OPERATION("usernameLinkOperation", new RateLimiterConfig(10, Duration.ofMinutes(1), false)),
    USERNAME_LINK_LOOKUP_PER_IP("usernameLinkLookupPerIp", new RateLimiterConfig(100, Duration.ofSeconds(15), true)),
    CHECK_ACCOUNT_EXISTENCE("checkAccountExistence", new RateLimiterConfig(1000, Duration.ofSeconds(4), true)),
    REGISTRATION("registration", new RateLimiterConfig(6, Duration.ofSeconds(30), false)),
    VERIFICATION_PUSH_CHALLENGE("verificationPushChallenge", new RateLimiterConfig(5, Duration.ofSeconds(30), false)),
    VERIFICATION_CAPTCHA("verificationCaptcha", new RateLimiterConfig(10, Duration.ofSeconds(30), false)),
    RATE_LIMIT_RESET("rateLimitReset", new RateLimiterConfig(2, Duration.ofHours(12), false)),
    CAPTCHA_CHALLENGE_ATTEMPT("captchaChallengeAttempt", new RateLimiterConfig(10, Duration.ofMinutes(144), false)),
    CAPTCHA_CHALLENGE_SUCCESS("captchaChallengeSuccess", new RateLimiterConfig(2, Duration.ofHours(12), false)),
    SET_BACKUP_ID("setBackupId", new RateLimiterConfig(10, Duration.ofHours(1), false)),
    SET_PAID_MEDIA_BACKUP_ID("setPaidMediaBackupId", new RateLimiterConfig(5, Duration.ofDays(7), false)),
    PUSH_CHALLENGE_ATTEMPT("pushChallengeAttempt", new RateLimiterConfig(10, Duration.ofMinutes(144), false)),
    PUSH_CHALLENGE_SUCCESS("pushChallengeSuccess", new RateLimiterConfig(2, Duration.ofHours(12), false)),
    GET_CALLING_RELAYS("getCallingRelays", new RateLimiterConfig(100, Duration.ofMinutes(10), false)),
    CREATE_CALL_LINK("createCallLink", new RateLimiterConfig(100, Duration.ofMinutes(15), false)),
    INBOUND_MESSAGE_BYTES("inboundMessageBytes", new RateLimiterConfig(128 * 1024 * 1024, Duration.ofNanos(500_000), true)),
    EXTERNAL_SERVICE_CREDENTIALS("externalServiceCredentials", new RateLimiterConfig(100, Duration.ofMinutes(15), false)),
    KEY_TRANSPARENCY_DISTINGUISHED_PER_IP("keyTransparencyDistinguished", new RateLimiterConfig(100, Duration.ofSeconds(15), true)),
    KEY_TRANSPARENCY_SEARCH_PER_IP("keyTransparencySearch", new RateLimiterConfig(100, Duration.ofSeconds(15), true)),
    KEY_TRANSPARENCY_MONITOR_PER_IP("keyTransparencyMonitor", new RateLimiterConfig(100, Duration.ofSeconds(15), true)),
    WAIT_FOR_LINKED_DEVICE("waitForLinkedDevice", new RateLimiterConfig(10, Duration.ofSeconds(30), false)),
    UPLOAD_TRANSFER_ARCHIVE("uploadTransferArchive", new RateLimiterConfig(10, Duration.ofMinutes(1), false)),
    WAIT_FOR_TRANSFER_ARCHIVE("waitForTransferArchive", new RateLimiterConfig(10, Duration.ofSeconds(30), false)),
    RECORD_DEVICE_TRANSFER_REQUEST("recordDeviceTransferRequest", new RateLimiterConfig(10, Duration.ofMillis(100), true)),
    WAIT_FOR_DEVICE_TRANSFER_REQUEST("waitForDeviceTransferRequest", new RateLimiterConfig(10, Duration.ofMillis(100), true)),
    DEVICE_CHECK_CHALLENGE("deviceCheckChallenge", new RateLimiterConfig(10, Duration.ofMinutes(1), false)),
    ;

    private final String id;

    private final RateLimiterConfig defaultConfig;

    For(final String id, final RateLimiterConfig defaultConfig) {
      this.id = id;
      this.defaultConfig = defaultConfig;
    }

    public String id() {
      return id;
    }

    public RateLimiterConfig defaultConfig() {
      return defaultConfig;
    }
  }

  public static RateLimiters create(
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final FaultTolerantRedisClusterClient cacheCluster) {
    return new RateLimiters(
        dynamicConfigurationManager, defaultScript(cacheCluster), cacheCluster, Clock.systemUTC());
  }

  @VisibleForTesting
  RateLimiters(
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final ClusterLuaScript validateScript,
      final FaultTolerantRedisClusterClient cacheCluster,
      final Clock clock) {
    super(For.values(), dynamicConfigurationManager, validateScript, cacheCluster, clock);
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

  public RateLimiter getKeyTransparencySearchLimiter() {
    return forDescriptor(For.KEY_TRANSPARENCY_SEARCH_PER_IP);
  }

  public RateLimiter getKeyTransparencyDistinguishedLimiter() {
    return forDescriptor(For.KEY_TRANSPARENCY_DISTINGUISHED_PER_IP);
  }

  public RateLimiter getKeyTransparencyMonitorLimiter() {
    return forDescriptor(For.KEY_TRANSPARENCY_MONITOR_PER_IP);
  }
}
