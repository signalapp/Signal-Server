/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.limits;


import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class RateLimiters {

  private final RateLimiter smsDestinationLimiter;
  private final RateLimiter voiceDestinationLimiter;
  private final RateLimiter voiceDestinationDailyLimiter;
  private final RateLimiter smsVoiceIpLimiter;
  private final RateLimiter smsVoicePrefixLimiter;
  private final RateLimiter autoBlockLimiter;
  private final RateLimiter verifyLimiter;
  private final RateLimiter pinLimiter;

  private final RateLimiter attachmentLimiter;
  private final RateLimiter contactsLimiter;
  private final RateLimiter contactsIpLimiter;
  private final RateLimiter preKeysLimiter;
  private final RateLimiter messagesLimiter;

  private final RateLimiter allocateDeviceLimiter;
  private final RateLimiter verifyDeviceLimiter;

  private final RateLimiter turnLimiter;

  private final RateLimiter profileLimiter;
  private final RateLimiter stickerPackLimiter;
  private final RateLimiter usernameLookupLimiter;
  private final RateLimiter usernameSetLimiter;

  public RateLimiters(RateLimitsConfiguration config, FaultTolerantRedisCluster cacheCluster) {
    this.smsDestinationLimiter = new RateLimiter(cacheCluster, "smsDestination",
                                                 config.getSmsDestination().getBucketSize(),
                                                 config.getSmsDestination().getLeakRatePerMinute());

    this.voiceDestinationLimiter = new RateLimiter(cacheCluster, "voxDestination",
                                                   config.getVoiceDestination().getBucketSize(),
                                                   config.getVoiceDestination().getLeakRatePerMinute());

    this.voiceDestinationDailyLimiter = new RateLimiter(cacheCluster, "voxDestinationDaily",
                                                        config.getVoiceDestinationDaily().getBucketSize(),
                                                        config.getVoiceDestinationDaily().getLeakRatePerMinute());

    this.smsVoiceIpLimiter = new RateLimiter(cacheCluster, "smsVoiceIp",
                                             config.getSmsVoiceIp().getBucketSize(),
                                             config.getSmsVoiceIp().getLeakRatePerMinute());

    this.smsVoicePrefixLimiter = new RateLimiter(cacheCluster, "smsVoicePrefix",
                                                 config.getSmsVoicePrefix().getBucketSize(),
                                                 config.getSmsVoicePrefix().getLeakRatePerMinute());

    this.autoBlockLimiter = new RateLimiter(cacheCluster, "autoBlock",
                                            config.getAutoBlock().getBucketSize(),
                                            config.getAutoBlock().getLeakRatePerMinute());

    this.verifyLimiter = new LockingRateLimiter(cacheCluster, "verify",
                                                config.getVerifyNumber().getBucketSize(),
                                                config.getVerifyNumber().getLeakRatePerMinute());

    this.pinLimiter = new LockingRateLimiter(cacheCluster, "pin",
                                             config.getVerifyPin().getBucketSize(),
                                             config.getVerifyPin().getLeakRatePerMinute());

    this.attachmentLimiter = new RateLimiter(cacheCluster, "attachmentCreate",
                                             config.getAttachments().getBucketSize(),
                                             config.getAttachments().getLeakRatePerMinute());

    this.contactsLimiter = new RateLimiter(cacheCluster, "contactsQuery",
                                           config.getContactQueries().getBucketSize(),
                                           config.getContactQueries().getLeakRatePerMinute());

    this.contactsIpLimiter = new RateLimiter(cacheCluster, "contactsIpQuery",
                                             config.getContactIpQueries().getBucketSize(),
                                             config.getContactIpQueries().getLeakRatePerMinute());

    this.preKeysLimiter = new RateLimiter(cacheCluster, "prekeys",
                                          config.getPreKeys().getBucketSize(),
                                          config.getPreKeys().getLeakRatePerMinute());

    this.messagesLimiter = new RateLimiter(cacheCluster, "messages",
                                           config.getMessages().getBucketSize(),
                                           config.getMessages().getLeakRatePerMinute());

    this.allocateDeviceLimiter = new RateLimiter(cacheCluster, "allocateDevice",
                                                 config.getAllocateDevice().getBucketSize(),
                                                 config.getAllocateDevice().getLeakRatePerMinute());

    this.verifyDeviceLimiter = new RateLimiter(cacheCluster, "verifyDevice",
                                               config.getVerifyDevice().getBucketSize(),
                                               config.getVerifyDevice().getLeakRatePerMinute());

    this.turnLimiter = new RateLimiter(cacheCluster, "turnAllocate",
                                       config.getTurnAllocations().getBucketSize(),
                                       config.getTurnAllocations().getLeakRatePerMinute());

    this.profileLimiter = new RateLimiter(cacheCluster, "profile",
                                          config.getProfile().getBucketSize(),
                                          config.getProfile().getLeakRatePerMinute());

    this.stickerPackLimiter = new RateLimiter(cacheCluster, "stickerPack",
                                              config.getStickerPack().getBucketSize(),
                                              config.getStickerPack().getLeakRatePerMinute());

    this.usernameLookupLimiter = new RateLimiter(cacheCluster, "usernameLookup",
                                                 config.getUsernameLookup().getBucketSize(),
                                                 config.getUsernameLookup().getLeakRatePerMinute());

    this.usernameSetLimiter = new RateLimiter(cacheCluster, "usernameSet",
                                              config.getUsernameSet().getBucketSize(),
                                              config.getUsernameSet().getLeakRatePerMinute());
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

  public RateLimiter getContactsLimiter() {
    return contactsLimiter;
  }

  public RateLimiter getContactsIpLimiter() {
    return contactsIpLimiter;
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

  public RateLimiter getAutoBlockLimiter() {
    return autoBlockLimiter;
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

  public RateLimiter getTurnLimiter() {
    return turnLimiter;
  }

  public RateLimiter getProfileLimiter() {
    return profileLimiter;
  }

  public RateLimiter getStickerPackLimiter() {
    return stickerPackLimiter;
  }

  public RateLimiter getUsernameLookupLimiter() {
    return usernameLookupLimiter;
  }

  public RateLimiter getUsernameSetLimiter() {
    return usernameSetLimiter;
  }

}
