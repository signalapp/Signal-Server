/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.webauthn4j.test.TestDataUtil;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class AnnotatedWebAuthnCredentialTest {

  private static AnnotatedWebAuthnCredential credential(final long counter, final byte[] metadataCiphertext) {
    return new AnnotatedWebAuthnCredential(
        TestDataUtil.createAttestedCredentialData(),
        counter,
        metadataCiphertext);
  }

  @Test
  void testEquals() {

    final AnnotatedWebAuthnCredential credential = credential(2, TestRandomUtil.nextBytes(160));

    final AnnotatedWebAuthnCredential sameCredential = new AnnotatedWebAuthnCredential(
        credential.getAttestedCredentialData(),
        credential.getCounter(),
        credential.metadataCiphertext().clone());

    assertEquals(credential, sameCredential);
  }

  @ParameterizedTest
  @MethodSource
  void serializationRoundTrips(final JsonRoundTrip roundTrip) throws JsonProcessingException {
    final AnnotatedWebAuthnCredential credential = credential(1, TestRandomUtil.nextBytes(160));

    assertEquals(credential, roundTrip.apply(credential));
  }

  static Collection<Arguments> serializationRoundTrips() {
    return List.of(
        argumentSet("AnnotatedWebAuthnCredential", (JsonRoundTrip) credential ->
            SystemMapper.jsonMapper().readValue(
                SystemMapper.jsonMapper().writeValueAsString(credential), AnnotatedWebAuthnCredential.class)),

        argumentSet("AnnotatedMfaKey JsonSubType", (JsonRoundTrip) credential ->
            SystemMapper.jsonMapper().readValue(
                SystemMapper.jsonMapper().writeValueAsString((AnnotatedMfaKey) credential), AnnotatedMfaKey.class))
    );
  }

  @FunctionalInterface
  private interface JsonRoundTrip {
    AnnotatedMfaKey apply(AnnotatedWebAuthnCredential credential) throws JsonProcessingException;
  }

}
