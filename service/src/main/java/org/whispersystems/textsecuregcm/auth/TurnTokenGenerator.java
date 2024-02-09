/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.calls.routing.TurnServerOptions;
import org.whispersystems.textsecuregcm.configuration.TurnUriConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicTurnConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.WeightedRandomSelect;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class TurnTokenGenerator {

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final byte[] turnSecret;

  private static final String ALGORITHM = "HmacSHA1";

  public TurnTokenGenerator(final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final byte[] turnSecret) {

    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.turnSecret = turnSecret;
  }

  public TurnToken generate(final UUID aci) {
    return generateToken(null, null, urls(aci));
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

      mac.init(new SecretKeySpec(turnSecret, ALGORITHM));
      final String password = Base64.getEncoder().encodeToString(mac.doFinal(userTime.getBytes()));

      return new TurnToken(userTime, password, urlsWithHostname, urlsWithIps, hostname);
    } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

  private List<String> urls(final UUID aci) {
    final DynamicTurnConfiguration turnConfig = dynamicConfigurationManager.getConfiguration().getTurnConfiguration();

    // Check if number is enrolled to test out specific turn servers
    final Optional<TurnUriConfiguration> enrolled = turnConfig.getUriConfigs().stream()
        .filter(config -> config.getEnrolledAcis().contains(aci))
        .findFirst();

    if (enrolled.isPresent()) {
      return enrolled.get().getUris();
    }

    // Otherwise, select from turn server sets by weighted choice
    return WeightedRandomSelect.select(turnConfig
        .getUriConfigs()
        .stream()
        .map(c -> new Pair<>(c.getUris(), c.getWeight())).toList());
  }
}
