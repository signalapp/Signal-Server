/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import javax.validation.Valid;
import org.whispersystems.textsecuregcm.configuration.TurnUriConfiguration;

public class DynamicTurnConfiguration {

  @JsonProperty
  private String hostname;

  /**
   * Rate at which to prioritize a random  turn URL to exercise all endpoints.
   * Based on a 100,000 basis, where 100,000 == 100%.
   */
  @JsonProperty
  private long randomizeRate = 5_000;

  @JsonProperty
  private List<@Valid TurnUriConfiguration> uriConfigs = Collections.emptyList();

  public List<TurnUriConfiguration> getUriConfigs() {
    return uriConfigs;
  }

  public long getRandomizeRate() {
    return randomizeRate;
  }

  public String getHostname() {
    return hostname;
  }
}
