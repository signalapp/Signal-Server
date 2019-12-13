package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class RemoteConfig {

  @JsonProperty
  @Pattern(regexp = "[A-Za-z0-9\\.]+")
  private String name;

  @JsonProperty
  @NotNull
  @Min(0)
  @Max(100)
  private int percentage;

  public RemoteConfig() {}

  public RemoteConfig(String name, int percentage) {
    this.name       = name;
    this.percentage = percentage;
  }

  public int getPercentage() {
    return percentage;
  }

  public String getName() {
    return name;
  }
}
