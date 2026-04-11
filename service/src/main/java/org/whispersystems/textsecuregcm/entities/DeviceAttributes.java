/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.whispersystems.textsecuregcm.util.RegistrationIdValidator.validRegistrationId;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import java.util.Set;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.DeviceCapabilityAdapter;

public record DeviceAttributes(
    boolean fetchesMessages,

    int registrationId,

    @JsonProperty("pniRegistrationId") int phoneNumberIdentityRegistrationId,

    @JsonSerialize(using = ByteArrayAdapter.Serializing.class)

    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @Size(max = 225)
    byte[] name,

    @JsonSerialize(using = DeviceCapabilityAdapter.Serializer.class)

    @JsonDeserialize(using = DeviceCapabilityAdapter.Deserializer.class)
    @Nullable
    Set<DeviceCapability> capabilities) {

  @AssertTrue
  @Schema(hidden = true)
  public boolean isEachRegistrationIdValid() {
    return validRegistrationId(registrationId) && validRegistrationId(phoneNumberIdentityRegistrationId);
  }
}
