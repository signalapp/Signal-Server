/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;

public class ChangePhoneNumberRequest {

  @JsonProperty
  @NotBlank
  final String number;

  @JsonProperty
  @NotBlank
  final String code;

  @JsonProperty("reglock")
  @Nullable
  final String registrationLock;

  @JsonProperty("device_messages")
  @Nullable
  final List<IncomingMessage> deviceMessages;

  @JsonProperty("device_signed_prekeys")
  @Nullable
  final Map<Long, SignedPreKey> deviceSignedPrekeys;

  @JsonCreator
  public ChangePhoneNumberRequest(@JsonProperty("number") final String number,
      @JsonProperty("code") final String code,
      @JsonProperty("reglock") @Nullable final String registrationLock,
      @JsonProperty("device_messages") @Nullable final List<IncomingMessage> deviceMessages,
      @JsonProperty("device_signed_prekeys") @Nullable final Map<Long, SignedPreKey> deviceSignedPrekeys) {

    this.number = number;
    this.code = code;
    this.registrationLock = registrationLock;
    this.deviceMessages = deviceMessages;
    this.deviceSignedPrekeys = deviceSignedPrekeys;
  }

  public String getNumber() {
    return number;
  }

  public String getCode() {
    return code;
  }

  @Nullable
  public String getRegistrationLock() {
    return registrationLock;
  }

  @Nullable
  public List<IncomingMessage> getDeviceMessages() {
    return deviceMessages;
  }

  @Nullable
  public Map<Long, SignedPreKey> getDeviceSignedPrekeys() {
    return deviceSignedPrekeys;
  }
}
