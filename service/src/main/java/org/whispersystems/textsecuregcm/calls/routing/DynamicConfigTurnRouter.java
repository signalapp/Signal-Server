/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import org.whispersystems.textsecuregcm.configuration.TurnUriConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicTurnConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.WeightedRandomSelect;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/** Uses DynamicConfig to help route a turn request */
public class DynamicConfigTurnRouter {

  private static final Random rng = new Random();

  public static final long RANDOMIZE_RATE_BASIS = 100_000;

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public DynamicConfigTurnRouter(final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public List<String> targetedUrls(final UUID aci) {
    final DynamicTurnConfiguration turnConfig = dynamicConfigurationManager.getConfiguration().getTurnConfiguration();

    final Optional<TurnUriConfiguration> enrolled = turnConfig.getUriConfigs().stream()
        .filter(config -> config.getEnrolledAcis().contains(aci))
        .findFirst();

      return enrolled
          .map(turnUriConfiguration -> turnUriConfiguration.getUris().stream().toList())
          .orElse(Collections.emptyList());
  }

  public List<String> randomUrls() {
    final DynamicTurnConfiguration turnConfig = dynamicConfigurationManager.getConfiguration().getTurnConfiguration();

    // select from turn server sets by weighted choice
    return WeightedRandomSelect.select(turnConfig
        .getUriConfigs()
        .stream()
        .map(c -> new Pair<>(c.getUris(), c.getWeight())).toList());
  }

  public String getHostname() {
    final DynamicTurnConfiguration turnConfig = dynamicConfigurationManager.getConfiguration().getTurnConfiguration();
    return turnConfig.getHostname();
  }

  public long getRandomizeRate() {
    final DynamicTurnConfiguration turnConfig = dynamicConfigurationManager.getConfiguration().getTurnConfiguration();
    return turnConfig.getRandomizeRate();
  }

  public int getDefaultInstanceIpCount() {
    final DynamicTurnConfiguration turnConfig = dynamicConfigurationManager.getConfiguration().getTurnConfiguration();
    return turnConfig.getDefaultInstanceIpCount();
  }

  public boolean shouldRandomize() {
    long rate = getRandomizeRate();
    return rate >= RANDOMIZE_RATE_BASIS || rng.nextLong(0, DynamicConfigTurnRouter.RANDOMIZE_RATE_BASIS) < rate;
  }
}
