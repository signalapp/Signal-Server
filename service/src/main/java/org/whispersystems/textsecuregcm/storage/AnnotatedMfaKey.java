/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value=AnnotatedTotpKey.class, name = "totp"),
    @JsonSubTypes.Type(value=AnnotatedWebAuthnCredential.class, name = "webauthn")})
public sealed interface AnnotatedMfaKey permits AnnotatedTotpKey, AnnotatedWebAuthnCredential {
  byte[] metadataCiphertext();
  AnnotatedMfaKey withMetadataCiphertext(byte[] newCiphertext);
}
