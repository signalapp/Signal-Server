/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.whispersystems.textsecuregcm.util.Util;

import java.security.MessageDigest;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class StoredVerificationCode {

  @JsonProperty
  private String code;

  @JsonProperty
  private long timestamp;

  @JsonProperty
  private String pushCode;

  @JsonProperty
  private String twilioVerificationSid;

  public StoredVerificationCode() {
  }

  public StoredVerificationCode(String code, long timestamp, String pushCode) {
    this(code, timestamp, pushCode, null);
  }

  public StoredVerificationCode(String code, long timestamp, String pushCode, String twilioVerificationSid) {
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
    if (timestamp + TimeUnit.MINUTES.toMillis(10) < System.currentTimeMillis()) {
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
