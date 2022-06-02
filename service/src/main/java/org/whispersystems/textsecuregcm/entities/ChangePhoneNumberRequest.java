/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
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

  @JsonProperty("deviceUpdates")
  @Nullable
  final Map<Long, DeviceUpdate> deviceUpdates;

  public static class DeviceUpdate {

    private final IncomingMessage message;
    private final SignedPreKey signedPhoneNumberIdentityPreKey;
    private final Integer registrationID;

    @JsonCreator
    public DeviceUpdate(
        @JsonProperty("message") final IncomingMessage message,
        @JsonProperty("signedPhoneNumberIdentityPrekey") final SignedPreKey signedPhoneNumberIdentityPreKey,
        @JsonProperty("registratonId") final Integer registrationID) {
      this.message = message;
      this.signedPhoneNumberIdentityPreKey = signedPhoneNumberIdentityPreKey;
      this.registrationID = registrationID;
    }

    public IncomingMessage getMessage() {
      return message;
    }

    public SignedPreKey getSignedPhoneNumberIdentityPreKey() {
      return signedPhoneNumberIdentityPreKey;
    }

    public Integer getRegistrationID() {
      return registrationID;
    }
  }

  @JsonCreator
  public ChangePhoneNumberRequest(@JsonProperty("number") final String number,
      @JsonProperty("code") final String code,
      @JsonProperty("reglock") @Nullable final String registrationLock,
      @JsonProperty("deviceUpdates") @Nullable final Map<Long, DeviceUpdate> deviceUpdates) {
    this.number = number;
    this.code = code;
    this.registrationLock = registrationLock;
    this.deviceUpdates = deviceUpdates;
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
  public Map<Long, DeviceUpdate> getDeviceUpdates() { return deviceUpdates; }
}
