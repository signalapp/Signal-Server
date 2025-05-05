/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;

public record PhoneNumberIdentityKeyDistributionRequest(
    @NotNull
    @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
    @Schema(description="the new identity key for this account's phone-number identity")
    IdentityKey pniIdentityKey,

    @NotNull
    @Valid
    @ArraySchema(
        arraySchema=@Schema(description="""
            A list of synchronization messages to send to companion devices to supply the private keys
            associated with the new identity key and their new prekeys.
            Exactly one message must be supplied for each device other than the sending (primary) device.
            """))
    List<@NotNull @Valid IncomingMessage> deviceMessages,

    @NotNull
    @NotEmpty
    @Valid
    @Schema(description="""
        A new signed elliptic-curve prekey for each device on the account, including this one.
        Each must be accompanied by a valid signature from the new identity key in this request.""")
    Map<Byte, @NotNull @Valid ECSignedPreKey> devicePniSignedPrekeys,

    @NotNull
    @NotEmpty
    @Valid
    @Schema(description="""
        A new signed post-quantum last-resort prekey for each device on the account, including this one.
        Each must be accompanied by a valid signature from the new identity key in this request.""")
    Map<Byte, @NotNull @Valid KEMSignedPreKey> devicePniPqLastResortPrekeys,

    @NotNull
    @NotEmpty
    @Valid
    @Schema(description="The new registration ID to use for the phone-number identity of each device, including this one.")
    Map<Byte, Integer> pniRegistrationIds) {

  public boolean isSignatureValidOnEachSignedPreKey(@Nullable final String userAgent) {
    final List<SignedPreKey<?>> signedPreKeys = new ArrayList<>(devicePniSignedPrekeys.values());
    signedPreKeys.addAll(devicePniPqLastResortPrekeys.values());

    return PreKeySignatureValidator.validatePreKeySignatures(pniIdentityKey, signedPreKeys, userAgent, "distribute-pni-keys");
  }
}
