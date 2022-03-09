package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicUakMigrationConfiguration {
  @JsonProperty
  private boolean enabled = true;

  public boolean isEnabled() {
    return enabled;
  }
}
