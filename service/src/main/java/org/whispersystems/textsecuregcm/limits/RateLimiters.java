/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;


import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class RateLimiters {

  public enum Handle {
    USERNAME_LOOKUP("usernameLookup"),
    CHECK_ACCOUNT_EXISTENCE("checkAccountExistence"),
    BACKUP_AUTH_CHECK;

    private final String id;


    Handle(final String id) {
      this.id = id;
    }

    Handle() {
      this.id = name();
    }

    public String id() {
      return id;
    }
  }

  private final RateLimiter smsDestinationLimiter;
  private final RateLimiter voiceDestinationLimiter;
  private final RateLimiter voiceDestinationDailyLimiter;
  private final RateLimiter smsVoiceIpLimiter;
  private final RateLimiter smsVoicePrefixLimiter;
  private final RateLimiter verifyLimiter;
  private final RateLimiter pinLimiter;
  private final RateLimiter registrationLimiter;
  private final RateLimiter attachmentLimiter;
  private final RateLimiter preKeysLimiter;
  private final RateLimiter messagesLimiter;
  private final RateLimiter allocateDeviceLimiter;
  private final RateLimiter verifyDeviceLimiter;
  private final RateLimiter turnLimiter;
  private final RateLimiter profileLimiter;
  private final RateLimiter stickerPackLimiter;
  private final RateLimiter artPackLimiter;
  private final RateLimiter usernameSetLimiter;
  private final RateLimiter usernameReserveLimiter;

  private final Map<String, RateLimiter> rateLimiterByHandle;

  public RateLimiters(final RateLimitsConfiguration config, final FaultTolerantRedisCluster cacheCluster) {
    this.smsDestinationLimiter = fromConfig("smsDestination", config.getSmsDestination(), cacheCluster);
    this.voiceDestinationLimiter = fromConfig("voxDestination", config.getVoiceDestination(), cacheCluster);
    this.voiceDestinationDailyLimiter = fromConfig("voxDestinationDaily", config.getVoiceDestinationDaily(), cacheCluster);
    this.smsVoiceIpLimiter = fromConfig("smsVoiceIp", config.getSmsVoiceIp(), cacheCluster);
    this.smsVoicePrefixLimiter = fromConfig("smsVoicePrefix", config.getSmsVoicePrefix(), cacheCluster);
    this.verifyLimiter = fromConfig("verify", config.getVerifyNumber(), cacheCluster);
    this.pinLimiter = fromConfig("pin", config.getVerifyPin(), cacheCluster);
    this.registrationLimiter = fromConfig("registration", config.getRegistration(), cacheCluster);
    this.attachmentLimiter = fromConfig("attachmentCreate", config.getAttachments(), cacheCluster);
    this.preKeysLimiter = fromConfig("prekeys", config.getPreKeys(), cacheCluster);
    this.messagesLimiter = fromConfig("messages", config.getMessages(), cacheCluster);
    this.allocateDeviceLimiter = fromConfig("allocateDevice", config.getAllocateDevice(), cacheCluster);
    this.verifyDeviceLimiter = fromConfig("verifyDevice", config.getVerifyDevice(), cacheCluster);
    this.turnLimiter = fromConfig("turnAllocate", config.getTurnAllocations(), cacheCluster);
    this.profileLimiter = fromConfig("profile", config.getProfile(), cacheCluster);
    this.stickerPackLimiter = fromConfig("stickerPack", config.getStickerPack(), cacheCluster);
    this.artPackLimiter = fromConfig("artPack", config.getArtPack(), cacheCluster);
    this.usernameSetLimiter = fromConfig("usernameSet", config.getUsernameSet(), cacheCluster);
    this.usernameReserveLimiter = fromConfig("usernameReserve", config.getUsernameReserve(), cacheCluster);

    this.rateLimiterByHandle = Stream.of(
        fromConfig(Handle.BACKUP_AUTH_CHECK.id(), config.getBackupAuthCheck(), cacheCluster),
        fromConfig(Handle.CHECK_ACCOUNT_EXISTENCE.id(), config.getCheckAccountExistence(), cacheCluster),
        fromConfig(Handle.USERNAME_LOOKUP.id(), config.getUsernameLookup(), cacheCluster)
    ).map(rl -> Pair.of(rl.name, rl)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  public Optional<RateLimiter> byHandle(final Handle handle) {
    return Optional.ofNullable(rateLimiterByHandle.get(handle.id()));
  }

  public RateLimiter getAllocateDeviceLimiter() {
    return allocateDeviceLimiter;
  }

  public RateLimiter getVerifyDeviceLimiter() {
    return verifyDeviceLimiter;
  }

  public RateLimiter getMessagesLimiter() {
    return messagesLimiter;
  }

  public RateLimiter getPreKeysLimiter() {
    return preKeysLimiter;
  }

  public RateLimiter getAttachmentLimiter() {
    return this.attachmentLimiter;
  }

  public RateLimiter getSmsDestinationLimiter() {
    return smsDestinationLimiter;
  }

  public RateLimiter getSmsVoiceIpLimiter() {
    return smsVoiceIpLimiter;
  }

  public RateLimiter getSmsVoicePrefixLimiter() {
    return smsVoicePrefixLimiter;
  }

  public RateLimiter getVoiceDestinationLimiter() {
    return voiceDestinationLimiter;
  }

  public RateLimiter getVoiceDestinationDailyLimiter() {
    return voiceDestinationDailyLimiter;
  }

  public RateLimiter getVerifyLimiter() {
    return verifyLimiter;
  }

  public RateLimiter getPinLimiter() {
    return pinLimiter;
  }

  public RateLimiter getRegistrationLimiter() {
    return registrationLimiter;
  }

  public RateLimiter getTurnLimiter() {
    return turnLimiter;
  }

  public RateLimiter getProfileLimiter() {
    return profileLimiter;
  }

  public RateLimiter getStickerPackLimiter() {
    return stickerPackLimiter;
  }

  public RateLimiter getArtPackLimiter() {
    return artPackLimiter;
  }

  public RateLimiter getUsernameLookupLimiter() {
    return byHandle(Handle.USERNAME_LOOKUP).orElseThrow();
  }

  public RateLimiter getUsernameSetLimiter() {
    return usernameSetLimiter;
  }

  public RateLimiter getUsernameReserveLimiter() {
    return usernameReserveLimiter;
  }

  public RateLimiter getCheckAccountExistenceLimiter() {
    return byHandle(Handle.CHECK_ACCOUNT_EXISTENCE).orElseThrow();
  }

  private static RateLimiter fromConfig(
      final String name,
      final RateLimitsConfiguration.RateLimitConfiguration cfg,
      final FaultTolerantRedisCluster cacheCluster) {
    return new RateLimiter(cacheCluster, name, cfg.getBucketSize(), cfg.getLeakRatePerMinute());
  }
}
