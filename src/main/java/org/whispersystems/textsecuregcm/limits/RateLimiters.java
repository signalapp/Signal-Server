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


import net.spy.memcached.MemcachedClient;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;

public class RateLimiters {

  private final RateLimiter smsDestinationLimiter;
  private final RateLimiter voiceDestinationLimiter;
  private final RateLimiter verifyLimiter;

  private final RateLimiter attachmentLimiter;
  private final RateLimiter contactsLimiter;
  private final RateLimiter preKeysLimiter;
  private final RateLimiter messagesLimiter;

  public RateLimiters(RateLimitsConfiguration config, MemcachedClient memcachedClient) {
    this.smsDestinationLimiter = new RateLimiter(memcachedClient, "smsDestination",
                                                 config.getSmsDestination().getBucketSize(),
                                                 config.getSmsDestination().getLeakRatePerMinute());

    this.voiceDestinationLimiter = new RateLimiter(memcachedClient, "voxDestination",
                                                   config.getVoiceDestination().getBucketSize(),
                                                   config.getVoiceDestination().getLeakRatePerMinute());

    this.verifyLimiter = new RateLimiter(memcachedClient, "verify",
                                         config.getVerifyNumber().getBucketSize(),
                                         config.getVerifyNumber().getLeakRatePerMinute());

    this.attachmentLimiter = new RateLimiter(memcachedClient, "attachmentCreate",
                                             config.getAttachments().getBucketSize(),
                                             config.getAttachments().getLeakRatePerMinute());

    this.contactsLimiter = new RateLimiter(memcachedClient, "contactsQuery",
                                           config.getContactQueries().getBucketSize(),
                                           config.getContactQueries().getLeakRatePerMinute());

    this.preKeysLimiter = new RateLimiter(memcachedClient, "prekeys",
                                          config.getPreKeys().getBucketSize(),
                                          config.getPreKeys().getLeakRatePerMinute());

    this.messagesLimiter = new RateLimiter(memcachedClient, "messages",
                                           config.getMessages().getBucketSize(),
                                           config.getMessages().getLeakRatePerMinute());

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

  public RateLimiter getVerifyLimiter() {
    return verifyLimiter;
  }

}
