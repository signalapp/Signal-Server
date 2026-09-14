/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.MfaFailureException;
import org.whispersystems.textsecuregcm.auth.webauthn.AuthenticationCeremonyParameters;
import org.whispersystems.textsecuregcm.auth.webauthn.WebAuthnAuthenticationParameters;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class MfaFailureExceptionMapperTest {

  private MfaFailureExceptionMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = new MfaFailureExceptionMapper();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void totp(final boolean hasTotpKey) throws JsonProcessingException {
    try (final Response response = mapper.toResponse(new MfaFailureException(null, hasTotpKey))) {
      assertEquals(441, response.getStatus());

      final MfaFailureExceptionMapper.MfaFailureResponse entity = (MfaFailureExceptionMapper.MfaFailureResponse) response.getEntity();
      assertNull(entity.webAuthnParameters());
      assertEquals(hasTotpKey, entity.hasTotpKey());
    }
  }

  @ParameterizedTest
  @MethodSource
  void webAuthn(final byte[] challenge, final List<byte[]> credentialIds) {

    try (final Response response = mapper.toResponse(new MfaFailureException(
        new AuthenticationCeremonyParameters(challenge, Duration.ofMinutes(2),credentialIds), false))) {

      final MfaFailureExceptionMapper.MfaFailureResponse entity = (MfaFailureExceptionMapper.MfaFailureResponse) response.getEntity();

      assertFalse(entity.hasTotpKey());

      assertNotNull(entity.webAuthnParameters());
      final WebAuthnAuthenticationParameters webAuthnParameters = entity.webAuthnParameters();

      assertArrayEquals(challenge, webAuthnParameters.challenge());
      assertEquals(Duration.ofMinutes(2).toSeconds(), webAuthnParameters.timeoutSeconds());

      assertEquals(
          credentialIds,
          webAuthnParameters.allowedCredentialIds());
    }
  }

  static Collection<Arguments> webAuthn() {
    return List.of(
        Arguments.argumentSet("no credentials", TestRandomUtil.nextBytes(16), List.of()),
        Arguments.argumentSet("one credential", TestRandomUtil.nextBytes(16), List.of(TestRandomUtil.nextBytes(32))),
        Arguments.argumentSet("two credentials", TestRandomUtil.nextBytes(16), List.of(TestRandomUtil.nextBytes(32), TestRandomUtil.nextBytes(32)))
    );
  }
}
