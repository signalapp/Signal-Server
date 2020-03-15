/*
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
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RateLimitsConfiguration {

  @JsonProperty
  private RateLimitConfiguration smsDestination = new RateLimitConfiguration(2, 2);

  @JsonProperty
  private RateLimitConfiguration voiceDestination = new RateLimitConfiguration(2, 1.0 / 2.0);

  @JsonProperty
  private RateLimitConfiguration voiceDestinationDaily = new RateLimitConfiguration(10, 10.0 / (24.0 * 60.0));

  @JsonProperty
  private RateLimitConfiguration smsVoiceIp = new RateLimitConfiguration(1000, 1000);

  @JsonProperty
  private RateLimitConfiguration smsVoicePrefix = new RateLimitConfiguration(1000, 1000);

  @JsonProperty
  private RateLimitConfiguration autoBlock = new RateLimitConfiguration(500, 500);

  @JsonProperty
  private RateLimitConfiguration verifyNumber = new RateLimitConfiguration(2, 2);

  @JsonProperty
  private RateLimitConfiguration verifyPin = new RateLimitConfiguration(10, 1 / (24.0 * 60.0));

  @JsonProperty
  private RateLimitConfiguration attachments = new RateLimitConfiguration(50, 50);

  @JsonProperty
  private RateLimitConfiguration contactQueries = new RateLimitConfiguration(50000, 50000);

  @JsonProperty
  private RateLimitConfiguration contactIpQueries = new RateLimitConfiguration(200, (100.0 / 60.0));

  @JsonProperty
  private RateLimitConfiguration prekeys = new RateLimitConfiguration(3, 1.0 / 10.0);

  @JsonProperty
  private RateLimitConfiguration messages = new RateLimitConfiguration(60, 60);

  @JsonProperty
  private RateLimitConfiguration allocateDevice = new RateLimitConfiguration(2, 1.0 / 2.0);

  @JsonProperty
  private RateLimitConfiguration verifyDevice = new RateLimitConfiguration(2, 2);

  @JsonProperty
  private RateLimitConfiguration turnAllocations = new RateLimitConfiguration(60, 60);

  @JsonProperty
  private RateLimitConfiguration profile = new RateLimitConfiguration(4320, 3);

  @JsonProperty
  private RateLimitConfiguration stickerPack = new RateLimitConfiguration(50, 20 / (24.0 * 60.0));

  @JsonProperty
  private RateLimitConfiguration usernameLookup = new RateLimitConfiguration(100, 100 / (24.0 * 60.0));

  @JsonProperty
  private RateLimitConfiguration usernameSet = new RateLimitConfiguration(100, 100 / (24.0 * 60.0));

  public RateLimitConfiguration getAutoBlock() {
    return autoBlock;
  }

  public RateLimitConfiguration getAllocateDevice() {
    return allocateDevice;
  }

  public RateLimitConfiguration getVerifyDevice() {
    return verifyDevice;
  }

  public RateLimitConfiguration getMessages() {
    return messages;
  }

  public RateLimitConfiguration getPreKeys() {
    return prekeys;
  }

  public RateLimitConfiguration getContactQueries() {
    return contactQueries;
  }

  public RateLimitConfiguration getContactIpQueries() {
    return contactIpQueries;
  }

  public RateLimitConfiguration getAttachments() {
    return attachments;
  }

  public RateLimitConfiguration getSmsDestination() {
    return smsDestination;
  }

  public RateLimitConfiguration getVoiceDestination() {
    return voiceDestination;
  }

  public RateLimitConfiguration getVoiceDestinationDaily() {
    return voiceDestinationDaily;
  }

  public RateLimitConfiguration getSmsVoiceIp() {
    return smsVoiceIp;
  }

  public RateLimitConfiguration getSmsVoicePrefix() {
    return smsVoicePrefix;
  }

  public RateLimitConfiguration getVerifyNumber() {
    return verifyNumber;
  }

  public RateLimitConfiguration getVerifyPin() {
    return verifyPin;
  }

  public RateLimitConfiguration getTurnAllocations() {
    return turnAllocations;
  }

  public RateLimitConfiguration getProfile() {
    return profile;
  }

  public RateLimitConfiguration getStickerPack() {
    return stickerPack;
  }

  public RateLimitConfiguration getUsernameLookup() {
    return usernameLookup;
  }

  public RateLimitConfiguration getUsernameSet() {
    return usernameSet;
  }

  public static class RateLimitConfiguration {
    @JsonProperty
    private int bucketSize;

    @JsonProperty
    private double leakRatePerMinute;

    public RateLimitConfiguration(int bucketSize, double leakRatePerMinute) {
      this.bucketSize        = bucketSize;
      this.leakRatePerMinute = leakRatePerMinute;
    }

    public RateLimitConfiguration() {}

    public int getBucketSize() {
      return bucketSize;
    }

    public double getLeakRatePerMinute() {
      return leakRatePerMinute;
    }
  }
}
