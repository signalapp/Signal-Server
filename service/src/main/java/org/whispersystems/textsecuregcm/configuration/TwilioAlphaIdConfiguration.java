package org.whispersystems.textsecuregcm.configuration;

import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.NotEmpty;

public class TwilioAlphaIdConfiguration {
  @NotEmpty
  private String prefix;

  @NotEmpty
  private String value;

  public String getPrefix() {
    return prefix;
  }

  @VisibleForTesting
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getValue() {
    return value;
  }

  @VisibleForTesting
  public void setValue(String value) {
    this.value = value;
  }
}
