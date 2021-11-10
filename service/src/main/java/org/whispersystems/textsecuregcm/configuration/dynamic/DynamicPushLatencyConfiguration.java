/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vdurmont.semver4j.Semver;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import java.util.Map;
import java.util.Set;

public class DynamicPushLatencyConfiguration {

  private final Map<ClientPlatform, Set<Semver>> instrumentedVersions;

  @JsonCreator
  public DynamicPushLatencyConfiguration(@JsonProperty("instrumentedVersions") final Map<ClientPlatform, Set<Semver>> instrumentedVersions) {
    this.instrumentedVersions = instrumentedVersions;
  }

  public Map<ClientPlatform, Set<Semver>> getInstrumentedVersions() {
    return instrumentedVersions;
  }
}
