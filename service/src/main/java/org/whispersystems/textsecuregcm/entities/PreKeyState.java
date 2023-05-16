/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class PreKeyState {

  @JsonProperty
  @Valid
  @Schema(description="A list of unsigned elliptic-curve prekeys to use for this device. " +
      "If present and not empty, replaces all stored unsigned EC prekeys for the device; " +
      "if absent or empty, any stored unsigned EC prekeys for the device are not deleted.")
  private List<PreKey> preKeys;

  @JsonProperty
  @Valid
  @Schema(description="An optional signed elliptic-curve prekey to use for this device. " +
      "If present, replaces the stored signed elliptic-curve prekey for the device; " +
      "if absent, the stored signed prekey is not deleted. " +
      "If present, must have a valid signature from the identity key in this request.")
  private SignedPreKey signedPreKey;

  @JsonProperty
  @Valid
  @Schema(description="A list of signed post-quantum one-time prekeys to use for this device. " +
      "Each key must have a valid signature from the identity key in this request. " +
      "If present and not empty, replaces all stored unsigned PQ prekeys for the device; " +
      "if absent or empty, any stored unsigned PQ prekeys for the device are not deleted.")
  private List<SignedPreKey> pqPreKeys;

  @JsonProperty
  @Valid
  @Schema(description="An optional signed last-resort post-quantum prekey to use for this device. " +
      "If present, replaces the stored signed post-quantum last-resort prekey for the device; " +
      "if absent, a stored last-resort prekey will *not* be deleted. " +
      "If present, must have a valid signature from the identity key in this request.")
  private SignedPreKey pqLastResortPreKey;

  @JsonProperty
  @NotEmpty
  @NotNull
  @Schema(description="Required. " +
      "The public identity key for this identity (account or phone-number identity). " +
      "If this device is not the primary device for the account, " +
      "must match the existing stored identity key for this identity.")
  private String identityKey;

  public PreKeyState() {}

  @VisibleForTesting
  public PreKeyState(String identityKey, SignedPreKey signedPreKey, List<PreKey> keys) {
    this(identityKey, signedPreKey, keys, null, null);
  }

  @VisibleForTesting
  public PreKeyState(String identityKey, SignedPreKey signedPreKey, List<PreKey> keys, List<SignedPreKey> pqKeys, SignedPreKey pqLastResortKey) {
    this.identityKey = identityKey;
    this.signedPreKey = signedPreKey;
    this.preKeys = keys;
    this.pqPreKeys = pqKeys;
    this.pqLastResortPreKey = pqLastResortKey;
  }

  public List<PreKey> getPreKeys() {
    return preKeys;
  }

  public SignedPreKey getSignedPreKey() {
    return signedPreKey;
  }

  public List<SignedPreKey> getPqPreKeys() {
    return pqPreKeys;
  }

  public SignedPreKey getPqLastResortPreKey() {
    return pqLastResortPreKey;
  }

  public String getIdentityKey() {
    return identityKey;
  }

  @AssertTrue
  public boolean isSignatureValidOnEachSignedKey() {
    List<SignedPreKey> spks = new ArrayList<>();
    if (pqPreKeys != null) {
      spks.addAll(pqPreKeys);
    }
    if (pqLastResortPreKey != null) {
      spks.add(pqLastResortPreKey);
    }
    if (signedPreKey != null) {
      spks.add(signedPreKey);
    }
    return spks.isEmpty() || PreKeySignatureValidator.validatePreKeySignatures(identityKey, spks);
  }
}
