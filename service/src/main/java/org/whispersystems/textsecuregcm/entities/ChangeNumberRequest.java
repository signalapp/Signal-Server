/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public record ChangeNumberRequest(
    @Schema(description="""
        A session ID from registration service, if using session id to authenticate this request.
        Must not be combined with `recoveryPassword`.""")
    String sessionId,

    @Schema(description="""
        The recovery password for the new phone number, if using a recovery password to authenticate this request.
        Must not be combined with `sessionId`.""")
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class) byte[] recoveryPassword,

    @Schema(description="the new phone number for this account")
    @NotBlank String number,

    @Schema(description="the registration lock password for the new phone number, if necessary")
    @JsonProperty("reglock") @Nullable String registrationLock,

    @Schema(description="the new public identity key to use for the phone-number identity associated with the new phone number")
    @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
    @NotEmpty byte[] pniIdentityKey,

    @Schema(description="""
        A list of synchronization messages to send to companion devices to supply the private keys
        associated with the new identity key and their new prekeys.
        Exactly one message must be supplied for each enabled device other than the sending (primary) device.""")
    @NotNull @Valid List<@NotNull @Valid IncomingMessage> deviceMessages,

    @Schema(description="""
        A new signed elliptic-curve prekey for each enabled device on the account, including this one.
        Each must be accompanied by a valid signature from the new identity key in this request.""")
    @NotNull @Valid Map<Long, @NotNull @Valid SignedPreKey> devicePniSignedPrekeys,

    @Schema(description="""
        A new signed post-quantum last-resort prekey for each enabled device on the account, including this one.
        May be absent, in which case the last resort PQ prekeys for each device will be deleted if any had been stored.
        If present, must contain one prekey per enabled device including this one.
        Prekeys for devices that did not previously have any post-quantum prekeys stored will be silently dropped.
        Each must be accompanied by a valid signature from the new identity key in this request.""")
    @Valid Map<Long, @NotNull @Valid SignedPreKey> devicePniPqLastResortPrekeys,

    @Schema(description="the new phone-number-identity registration ID for each enabled device on the account, including this one")
    @NotNull Map<Long, Integer> pniRegistrationIds) implements PhoneVerificationRequest {

  @AssertTrue
  public boolean isSignatureValidOnEachSignedPreKey() {
    List<SignedPreKey> spks = new ArrayList<>();
    if (devicePniSignedPrekeys != null) {
      spks.addAll(devicePniSignedPrekeys.values());
    }
    if (devicePniPqLastResortPrekeys != null) {
      spks.addAll(devicePniPqLastResortPrekeys.values());
    }
    return spks.isEmpty() || PreKeySignatureValidator.validatePreKeySignatures(pniIdentityKey, spks);
  }
}
