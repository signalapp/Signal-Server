/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record PhoneNumberIdentityKeyDistributionRequest(
        @NotBlank
        @Schema(description="the new identity key for this account's phone-number identity")
        String pniIdentityKey,
    
        @NotNull
        @Valid
        @Schema(description="A message for each companion device to pass its new private keys")
        List<@NotNull @Valid IncomingMessage> deviceMessages,
    
        @NotNull
        @Valid
        @Schema(description="The public key of a new signed elliptic-curve prekey pair for each device")
        Map<Long, @NotNull @Valid SignedPreKey> devicePniSignedPrekeys,
    
        @NotNull
        @Valid
        @Schema(description="The new registration ID to use for the phone-number identity of each device")
        Map<Long, Integer> pniRegistrationIds) {
}
