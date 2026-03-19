/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.IncorrectClaimException;
import com.auth0.jwt.interfaces.DecodedJWT;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class JwtGeneratorTest {
  private static final byte[] SECRET = TestRandomUtil.nextBytes(32);
  private static final String AUD = "test-audience";
  private static final String SUB = "test-key";

  @Test
  public void validToken() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, _ -> {});

    final DecodedJWT decoded = JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(AUD)
        .withSubject(SUB)
        .build().verify(token);
    assertThat(decoded.getIssuedAt()).isEqualTo(Instant.EPOCH);
    assertThat(decoded.getClaims()).containsOnlyKeys("aud", "sub", "iat");
  }

  @Test
  public void validClaims() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, builder -> builder
        .withClaim("number", 17)
        .withClaim("string", "abc"));

    final DecodedJWT decoded = JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(AUD)
        .withSubject(SUB)
        .withClaim("number", 17)
        .withClaim("string", "abc")
        .build().verify(token);
    assertThat(decoded.getIssuedAt()).isEqualTo(Instant.EPOCH);
    assertThat(decoded.getClaims()).containsOnlyKeys("aud", "sub", "iat", "number", "string");
  }

  @Test
  public void badAudience() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt("bad-audience", SUB, _ -> {});

    assertThatExceptionOfType(IncorrectClaimException.class).isThrownBy(() -> JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(AUD)
        .withSubject(SUB)
        .build().verify(token));
  }

  @Test
  public void badCustomClaim() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, builder -> builder
        .withClaim("number", 18));

    assertThatExceptionOfType(IncorrectClaimException.class).isThrownBy(() -> JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(AUD)
        .withSubject(SUB)
        .withClaim("number", 17)
        .build().verify(token));
  }

}
