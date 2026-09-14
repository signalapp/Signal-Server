/**
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.webauthn4j.credential.CredentialRecord;
import com.webauthn4j.data.attestation.authenticator.AttestedCredentialData;
import com.webauthn4j.data.client.CollectedClientData;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

public final class AnnotatedWebAuthnCredential implements AnnotatedMfaKey, CredentialRecord {

  @JsonSerialize(using = AttestedCredentialDataAdapter.Serializer.class)
  @JsonDeserialize(using = AttestedCredentialDataAdapter.Deserializer.class)
  private final AttestedCredentialData attestedCredentialData;

  private long counter;

  @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
  private final byte[] metadataCiphertext;

  @JsonCreator
  public AnnotatedWebAuthnCredential(
      @JsonProperty("attestedCredentialData") final AttestedCredentialData attestedCredentialData,
      @JsonProperty("counter") final long counter,
      @JsonProperty("metadataCiphertext") final byte[] metadataCiphertext) {

    this.attestedCredentialData = Objects.requireNonNull(attestedCredentialData);
    this.counter = counter;
    this.metadataCiphertext = metadataCiphertext;
  }

  @Override
  public byte[] metadataCiphertext() {
    return metadataCiphertext;
  }

  public byte[] getCredentialId() {
    return attestedCredentialData.getCredentialId();
  }

  @Override
  public AnnotatedWebAuthnCredential withMetadataCiphertext(final byte[] newMetadataCiphertext) {
    return new AnnotatedWebAuthnCredential(attestedCredentialData, counter, newMetadataCiphertext);
  }

  /// Always `null` in this implementation
  @Override
  public Boolean isUvInitialized() {
    return null;
  }

  @Nonnull
  @Override
  public AttestedCredentialData getAttestedCredentialData() {
    return attestedCredentialData;
  }

  /// Always `null` in this implementation
  @Nullable
  @Override
  public CollectedClientData getClientData() {
    return null;
  }

  @Override
  public long getCounter() {
    return counter;
  }

  /// Always `null` in this implementation
  @Override
  public Boolean isBackupEligible() {
    return null;
  }

  /// Always `null` in this implementation
  @Override
  public Boolean isBackedUp() {
    return null;
  }

  @Override
  public void setBackedUp(boolean backedUp) {
    // not used in this implementation
  }

  @Override
  public void setCounter(final long counter) {
    this.counter = counter;
  }

  @Override
  public void setUvInitialized(boolean uvInitialized) {
    // not used in this implementation
  }

  @Override
  public void setBackupEligible(boolean ignored) {
    // not used in this implementation
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof AnnotatedWebAuthnCredential that)) {
      return false;
    }

    return counter == that.counter
        && Arrays.equals(metadataCiphertext, that.metadataCiphertext)
        && Objects.equals(attestedCredentialData, that.attestedCredentialData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attestedCredentialData, counter, Arrays.hashCode(metadataCiphertext));
  }
}
