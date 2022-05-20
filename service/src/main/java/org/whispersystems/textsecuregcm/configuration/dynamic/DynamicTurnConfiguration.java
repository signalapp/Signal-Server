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
  private String secret;

  @JsonProperty
  private List<@Valid TurnUriConfiguration> uriConfigs = Collections.emptyList();

  public List<TurnUriConfiguration> getUriConfigs() {
    return uriConfigs;
  }

  public String getSecret() {
    return secret;
  }
}
