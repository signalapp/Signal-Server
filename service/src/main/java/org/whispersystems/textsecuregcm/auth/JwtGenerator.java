
/*
 * Copyright 2026 Signal Messenger, LLC
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import java.time.Clock;
import java.time.Instant;
import java.util.function.Consumer;

/// Generates JWT tokens for external services to validate. This class always uses symmetric HMAC-SHA256 with a shared secret
public class JwtGenerator {

  public static final String MAX_LENGTH_CLAIM_KEY = "maxLen";
  public static final String SCOPE_CLAIM_KEY = "scope";

  private final Algorithm algorithm;
  private final Clock clock;

  public JwtGenerator(final byte[] sharedSecret, final Clock clock) {
    this.algorithm = Algorithm.HMAC256(sharedSecret);
    this.clock = clock;
  }

  public String generateJwt(final String audience, final String subject, Consumer<JWTCreator.Builder> claimCustomizer) {
    final Instant now = clock.instant();
    JWTCreator.Builder builder = JWT.create()
        .withAudience(audience)
        .withSubject(subject)
        .withIssuedAt(now);
    claimCustomizer.accept(builder);
    return builder.sign(algorithm);
  }
}
