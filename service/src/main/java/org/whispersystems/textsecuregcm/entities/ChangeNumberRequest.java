/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record ChangeNumberRequest(String sessionId,
                                  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class) byte[] recoveryPassword,
                                  @NotBlank String number,
                                  @JsonProperty("reglock") @Nullable String registrationLock,
                                  @NotBlank String pniIdentityKey,
                                  @NotNull @Valid List<@NotNull @Valid IncomingMessage> deviceMessages,
                                  @NotNull @Valid Map<Long, @NotNull @Valid SignedPreKey> devicePniSignedPrekeys,
                                  @NotNull Map<Long, Integer> pniRegistrationIds) implements PhoneVerificationRequest {

  @AssertTrue
  public boolean isSignatureValidOnEachSignedPreKey() {
    if (devicePniSignedPrekeys == null) {
      return true;
    }
    return devicePniSignedPrekeys.values().parallelStream()
        .allMatch(spk -> PreKeySignatureValidator.validatePreKeySignature(pniIdentityKey, spk));
  }
}
