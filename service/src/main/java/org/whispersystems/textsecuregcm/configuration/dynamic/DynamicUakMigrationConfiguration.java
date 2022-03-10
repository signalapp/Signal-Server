package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicUakMigrationConfiguration {
  @JsonProperty
  private boolean enabled = true;

  @JsonProperty
  private int maxOutstandingNormalizes = 25;

  public boolean isEnabled() {
    return enabled;
  }

  public int getMaxOutstandingNormalizes() {
    return maxOutstandingNormalizes;
  }
}
