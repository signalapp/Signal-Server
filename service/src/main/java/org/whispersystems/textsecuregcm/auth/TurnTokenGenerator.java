/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

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
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TurnTokenGenerator {

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfiguration;

  private static final String ALGORITHM = "HmacSHA1";

  public TurnTokenGenerator(final DynamicConfigurationManager<DynamicConfiguration> config) {
    this.dynamicConfiguration = config;
  }

  public TurnToken generate(final String e164) {
    try {
      final byte[] key = dynamicConfiguration.getConfiguration().getTurnConfiguration().getSecret().getBytes();
      final List<String> urls = urls(e164);
      final Mac mac = Mac.getInstance(ALGORITHM);
      final long validUntilSeconds = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)) / 1000;
      final long user = Util.ensureNonNegativeInt(new SecureRandom().nextInt());
      final String userTime = validUntilSeconds + ":" + user;

      mac.init(new SecretKeySpec(key, ALGORITHM));
      final String password = Base64.getEncoder().encodeToString(mac.doFinal(userTime.getBytes()));

      return new TurnToken(userTime, password, urls);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

  private List<String> urls(final String e164) {
    final DynamicTurnConfiguration turnConfig = dynamicConfiguration.getConfiguration().getTurnConfiguration();

    // Check if number is enrolled to test out specific turn servers
    final Optional<TurnUriConfiguration> enrolled = turnConfig.getUriConfigs().stream()
        .filter(config -> config.getEnrolledNumbers().contains(e164))
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
