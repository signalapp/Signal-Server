package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import javax.validation.constraints.NotEmpty;

public class TwilioVerificationTextConfiguration {

  @JsonProperty
  @NotEmpty
  private String ios;

  @JsonProperty
  @NotEmpty
  private String androidNg;

  @JsonProperty
  @NotEmpty
  private String android202001;

  @JsonProperty
  @NotEmpty
  private String android202103;

  @JsonProperty
  @NotEmpty
  private String generic;

  public String getIosText() {
    return ios;
  }

  public void setIosText(String ios) {
    this.ios = ios;
  }

  public String getAndroidNgText() {
    return androidNg;
  }

  public void setAndroidNgText(final String androidNg) {
    this.androidNg = androidNg;
  }

  public String getAndroid202001Text() {
    return android202001;
  }

  public void setAndroid202001Text(final String android202001) {
    this.android202001 = android202001;
  }

  public String getAndroid202103Text() {
    return android202103;
  }

  public void setAndroid202103Text(final String android202103) {
    this.android202103 = android202103;
  }

  public String getGenericText() {
    return generic;
  }

  public void setGenericText(final String generic) {
    this.generic = generic;
  }
}
