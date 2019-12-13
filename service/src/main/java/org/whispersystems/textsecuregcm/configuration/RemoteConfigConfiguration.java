package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.LinkedList;
import java.util.List;

public class RemoteConfigConfiguration {

  @JsonProperty
  @NotNull
  private List<String> authorizedTokens = new LinkedList<>();

  public List<String> getAuthorizedTokens() {
    return authorizedTokens;
  }
}
