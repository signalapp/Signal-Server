/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.util.Util;

public class TurnTokenGenerator {

  private final byte[] turnSecret;

  private static final String ALGORITHM = "HmacSHA1";

  private static final String WithUrlsProtocol = "00";

  private static final String WithIpsProtocol = "01";

  public TurnTokenGenerator(final byte[] turnSecret) {
    this.turnSecret = turnSecret;
  }

  public TurnToken generateWithTurnServerOptions(TurnServerOptions options) {
    return generateToken(options.hostname(), options.urlsWithIps(), options.urlsWithHostname());
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private TurnToken generateToken(
      String hostname,
      Optional<List<String>> urlsWithIps,
      Optional<List<String>> urlsWithHostname
  ) {
    try {
      final Mac mac = Mac.getInstance(ALGORITHM);
      final long validUntilSeconds = Instant.now().plus(Duration.ofDays(1)).getEpochSecond();
      final long user = Util.ensureNonNegativeInt(new SecureRandom().nextInt());
      final String userTime = validUntilSeconds + ":" + user;
      final String protocol = urlsWithIps.isEmpty() || urlsWithIps.get().isEmpty()
          ? WithUrlsProtocol
          : WithIpsProtocol;
      final String protocolUserTime = userTime + "#" + protocol;

      mac.init(new SecretKeySpec(turnSecret, ALGORITHM));
      final String password = Base64.getEncoder().encodeToString(mac.doFinal(protocolUserTime.getBytes()));

      return from(
          protocolUserTime,
          password,
          urlsWithHostname,
          urlsWithIps,
          hostname
      );
    } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static TurnToken from(
      String username,
      String password,
      Optional<List<String>> urls,
      Optional<List<String>> urlsWithIps,
      String hostname
  ) {
    return new TurnToken(
        username,
        password,
        urls.orElse(Collections.emptyList()),
        urlsWithIps.orElse(Collections.emptyList()),
        hostname
    );
  }
}
