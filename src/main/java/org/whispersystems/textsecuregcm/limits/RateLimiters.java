/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.limits;


import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;

import redis.clients.jedis.JedisPool;

public class RateLimiters {

  private final RateLimiter smsDestinationLimiter;
  private final RateLimiter voiceDestinationLimiter;
  private final RateLimiter voiceDestinationDailyLimiter;
  private final RateLimiter verifyLimiter;
  private final RateLimiter pinLimiter;

  private final RateLimiter attachmentLimiter;
  private final RateLimiter contactsLimiter;
  private final RateLimiter preKeysLimiter;
  private final RateLimiter messagesLimiter;

  private final RateLimiter allocateDeviceLimiter;
  private final RateLimiter verifyDeviceLimiter;

  private final RateLimiter turnLimiter;

  private final RateLimiter profileLimiter;

  public RateLimiters(RateLimitsConfiguration config, JedisPool cacheClient) {
    this.smsDestinationLimiter = new RateLimiter(cacheClient, "smsDestination",
                                                 config.getSmsDestination().getBucketSize(),
                                                 config.getSmsDestination().getLeakRatePerMinute());

    this.voiceDestinationLimiter = new RateLimiter(cacheClient, "voxDestination",
                                                   config.getVoiceDestination().getBucketSize(),
                                                   config.getVoiceDestination().getLeakRatePerMinute());

    this.voiceDestinationDailyLimiter = new RateLimiter(cacheClient, "voxDestinationDaily",
                                                        config.getVoiceDestinationDaily().getBucketSize(),
                                                        config.getVoiceDestinationDaily().getLeakRatePerMinute());

    this.verifyLimiter = new RateLimiter(cacheClient, "verify",
                                         config.getVerifyNumber().getBucketSize(),
                                         config.getVerifyNumber().getLeakRatePerMinute());

    this.pinLimiter = new LockingRateLimiter(cacheClient, "pin",
                                             config.getVerifyPin().getBucketSize(),
                                             config.getVerifyPin().getLeakRatePerMinute());

    this.attachmentLimiter = new RateLimiter(cacheClient, "attachmentCreate",
                                             config.getAttachments().getBucketSize(),
                                             config.getAttachments().getLeakRatePerMinute());

    this.contactsLimiter = new RateLimiter(cacheClient, "contactsQuery",
                                           config.getContactQueries().getBucketSize(),
                                           config.getContactQueries().getLeakRatePerMinute());

    this.preKeysLimiter = new RateLimiter(cacheClient, "prekeys",
                                          config.getPreKeys().getBucketSize(),
                                          config.getPreKeys().getLeakRatePerMinute());

    this.messagesLimiter = new RateLimiter(cacheClient, "messages",
                                           config.getMessages().getBucketSize(),
                                           config.getMessages().getLeakRatePerMinute());

    this.allocateDeviceLimiter = new RateLimiter(cacheClient, "allocateDevice",
                                                 config.getAllocateDevice().getBucketSize(),
                                                 config.getAllocateDevice().getLeakRatePerMinute());

    this.verifyDeviceLimiter = new RateLimiter(cacheClient, "verifyDevice",
                                               config.getVerifyDevice().getBucketSize(),
                                               config.getVerifyDevice().getLeakRatePerMinute());

    this.turnLimiter = new RateLimiter(cacheClient, "turnAllocate",
                                       config.getTurnAllocations().getBucketSize(),
                                       config.getTurnAllocations().getLeakRatePerMinute());

    this.profileLimiter = new RateLimiter(cacheClient, "profile",
                                          config.getProfile().getBucketSize(),
                                          config.getProfile().getLeakRatePerMinute());
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

  public RateLimiter getAttachmentLimiter() {
    return this.attachmentLimiter;
  }

  public RateLimiter getSmsDestinationLimiter() {
    return smsDestinationLimiter;
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

}
