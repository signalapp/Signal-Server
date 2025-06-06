/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.E164;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;
import org.whispersystems.textsecuregcm.util.RegistrationIdValidator;

public record ChangeNumberRequest(
    @Schema(description="""
        A session ID from registration service, if using session id to authenticate this request.
        Must not be combined with `recoveryPassword`.""")
    String sessionId,

    @Schema(type="string", description="""
        The base64-encoded recovery password for the new phone number, if using a recovery password to authenticate this request.
        Must not be combined with `sessionId`.""")
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class) byte[] recoveryPassword,

    @E164
    @Schema(description="the new phone number for this account")
    @NotBlank String number,

    @Schema(description="the registration lock password for the new phone number, if necessary")
    @JsonProperty("reglock") @Nullable String registrationLock,

    @Schema(description="the new public identity key to use for the phone-number identity associated with the new phone number")
    @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
    @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
    @NotNull IdentityKey pniIdentityKey,

    @ArraySchema(
        arraySchema=@Schema(description="""
        A list of synchronization messages to send to companion devices to supply the private keysManager
        associated with the new identity key and their new prekeys.
        Exactly one message must be supplied for each device other than the sending (primary) device."""))
    @NotNull @Valid List<@NotNull @Valid IncomingMessage> deviceMessages,

    @Schema(description="""
        A new signed elliptic-curve prekey for each device on the account, including this one.
        Each must be accompanied by a valid signature from the new identity key in this request.""")
    @NotNull @NotEmpty @Valid Map<Byte, @NotNull @Valid ECSignedPreKey> devicePniSignedPrekeys,

    @Schema(description="""
        A new signed post-quantum last-resort prekey for each device on the account, including this one.
        Each must be accompanied by a valid signature from the new identity key in this request.""")
    @NotNull @NotEmpty @Valid Map<Byte, @NotNull @Valid KEMSignedPreKey> devicePniPqLastResortPrekeys,

    @Schema(description="the new phone-number-identity registration ID for each device on the account, including this one")
    @NotNull @NotEmpty Map<Byte, Integer> pniRegistrationIds) implements PhoneVerificationRequest {

  public boolean isSignatureValidOnEachSignedPreKey(@Nullable final String userAgent) {
    final List<SignedPreKey<?>> spks = new ArrayList<>(devicePniSignedPrekeys.values());
    spks.addAll(devicePniPqLastResortPrekeys.values());

    return PreKeySignatureValidator.validatePreKeySignatures(pniIdentityKey, spks, userAgent, "change-number");
  }

  @AssertTrue
  @Schema(hidden = true)
  public boolean isEachPniRegistrationIdValid() {
    return pniRegistrationIds == null || pniRegistrationIds.values().stream().allMatch(RegistrationIdValidator::validRegistrationId);
  }
}
