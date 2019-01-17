package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VoiceVerificationConfiguration {

  @JsonProperty
  @Valid
  @NotEmpty
  private String url;

  @JsonProperty
  @Valid
  @NotNull
  private List<String> locales;

  public String getUrl() {
    return url;
  }

  public Set<String> getLocales() {
    return new HashSet<>(locales);
  }
}
