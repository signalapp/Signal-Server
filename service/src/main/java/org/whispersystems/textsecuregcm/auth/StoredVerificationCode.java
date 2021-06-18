/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.util.Util;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class StoredVerificationCode {

  @JsonProperty
  private final String code;

  @JsonProperty
  private final long timestamp;

  @JsonProperty
  private final String pushCode;

  @JsonProperty
  @Nullable
  private final String twilioVerificationSid;

  public static final Duration EXPIRATION = Duration.ofMinutes(10);

  @JsonCreator
  public StoredVerificationCode(
      @JsonProperty("code") final String code,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("pushCode") final String pushCode,
      @JsonProperty("twilioVerificationSid") @Nullable final String twilioVerificationSid) {

    this.code = code;
    this.timestamp = timestamp;
    this.pushCode = pushCode;
    this.twilioVerificationSid = twilioVerificationSid;
  }

  public String getCode() {
    return code;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getPushCode() {
    return pushCode;
  }

  public Optional<String> getTwilioVerificationSid() {
    return Optional.ofNullable(twilioVerificationSid);
  }

  public boolean isValid(String theirCodeString) {
    return isValid(theirCodeString, Instant.now());
  }

  @VisibleForTesting
  boolean isValid(String theirCodeString, Instant currentTime) {
    if (Instant.ofEpochMilli(timestamp).plus(EXPIRATION).isBefore(currentTime)) {
      return false;
    }

    if (Util.isEmpty(code) || Util.isEmpty(theirCodeString)) {
      return false;
    }

    byte[] ourCode = code.getBytes();
    byte[] theirCode = theirCodeString.getBytes();

    return MessageDigest.isEqual(ourCode, theirCode);
  }
}
