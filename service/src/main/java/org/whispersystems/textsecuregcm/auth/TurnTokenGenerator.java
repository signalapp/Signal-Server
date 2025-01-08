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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.configuration.TurnUriConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicTurnConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.WeightedRandomSelect;

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

  private TurnToken generateToken(String hostname, List<String> urlsWithIps, List<String> urlsWithHostname) {
    try {
      final Mac mac = Mac.getInstance(ALGORITHM);
      final long validUntilSeconds = Instant.now().plus(Duration.ofDays(1)).getEpochSecond();
      final long user = Util.ensureNonNegativeInt(new SecureRandom().nextInt());
      final String userTime = validUntilSeconds + ":" + user;
      final String protocol = urlsWithIps != null && !urlsWithIps.isEmpty()
          ? WithIpsProtocol
          : WithUrlsProtocol;
      final String protocolUserTime = userTime + "#" + protocol;

      mac.init(new SecretKeySpec(turnSecret, ALGORITHM));
      final String password = Base64.getEncoder().encodeToString(mac.doFinal(protocolUserTime.getBytes()));

      return new TurnToken(protocolUserTime, password, urlsWithHostname, urlsWithIps, hostname);
    } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }
}
