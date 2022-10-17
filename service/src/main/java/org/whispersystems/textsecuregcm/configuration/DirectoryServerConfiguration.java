/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.List;

public class DirectoryServerConfiguration {

  @NotEmpty
  @JsonProperty
  private String replicationName;

  @NotEmpty
  @JsonProperty
  private String replicationUrl;

  @NotEmpty
  @JsonProperty
  private String replicationPassword;

  @NotEmpty
  @JsonProperty
  private List<@NotBlank String> replicationCaCertificates;

  public String getReplicationName() {
    return replicationName;
  }

  public String getReplicationUrl() {
    return replicationUrl;
  }

  public String getReplicationPassword() {
    return replicationPassword;
  }

  public List<String> getReplicationCaCertificates() {
    return replicationCaCertificates;
  }

}
