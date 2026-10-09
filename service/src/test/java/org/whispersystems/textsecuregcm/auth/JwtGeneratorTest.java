/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.IncorrectClaimException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Verification;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class JwtGeneratorTest {
  private static final byte[] SECRET = TestRandomUtil.nextBytes(32);
  private static final String AUD = "test-audience";
  private static final String SUB = "test-key";
  private static final Duration EXPIRATION = Duration.ofHours(1);

  private static Verification verifier() {
    return JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(AUD)
        .withSubject(SUB);
  }

  private static DecodedJWT verify(final Verification verification, final TestClock clock, final String token) {
    return ((JWTVerifier.BaseVerification) verification).build(clock).verify(token);
  }

  @Test
  public void validToken() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, EXPIRATION, _ -> {});

    final DecodedJWT decoded = verify(verifier(), clock, token);
    assertThat(decoded.getIssuedAtAsInstant()).isEqualTo(Instant.EPOCH);
    assertThat(decoded.getExpiresAtAsInstant()).isEqualTo(Instant.EPOCH.plus(EXPIRATION));
    assertThat(decoded.getClaims()).containsOnlyKeys("aud", "sub", "iat", "exp");
  }

  @Test
  public void validClaims() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, EXPIRATION, builder -> builder
        .withClaim("number", 17)
        .withClaim("string", "abc"));

    final DecodedJWT decoded = verify(verifier()
        .withClaim("number", 17)
        .withClaim("string", "abc"), clock, token);
    assertThat(decoded.getIssuedAtAsInstant()).isEqualTo(Instant.EPOCH);
    assertThat(decoded.getClaims()).containsOnlyKeys("aud", "sub", "iat", "exp", "number", "string");
  }

  @Test
  public void badAudience() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt("bad-audience", SUB, EXPIRATION, _ -> {});

    assertThatExceptionOfType(IncorrectClaimException.class).isThrownBy(() -> verify(verifier(), clock, token));
  }

  @Test
  public void badCustomClaim() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, EXPIRATION, builder -> builder
        .withClaim("number", 18));

    assertThatExceptionOfType(IncorrectClaimException.class).isThrownBy(() -> verify(verifier()
        .withClaim("number", 17), clock, token));
  }

  @Test
  public void expiredToken() {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final JwtGenerator jwtGenerator = new JwtGenerator(SECRET, clock);
    final String token = jwtGenerator.generateJwt(AUD, SUB, EXPIRATION, _ -> {});

    clock.pin(Instant.EPOCH.plus(EXPIRATION).minusSeconds(1));
    assertThat(verify(verifier(), clock, token).getSubject()).isEqualTo(SUB);

    clock.pin(Instant.EPOCH.plus(EXPIRATION).plus(Duration.ofSeconds(1)));
    assertThatExceptionOfType(TokenExpiredException.class).isThrownBy(() -> verify(verifier(), clock, token));
  }

}
