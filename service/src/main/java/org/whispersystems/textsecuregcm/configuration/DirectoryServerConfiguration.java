/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class DirectoryServerConfiguration {

  @NotEmpty
  @JsonProperty
  private String replicationName;

  @JsonProperty
  private boolean replicationPrimary;

  @NotEmpty
  @JsonProperty
  private String replicationUrl;

  @NotEmpty
  @JsonProperty
  private String replicationPassword;

  @NotEmpty
  @JsonProperty
  private String replicationCaCertificate;

  public String getReplicationName() {
    return replicationName;
  }

  public boolean isReplicationPrimary() {
    return replicationPrimary;
  }

  public String getReplicationUrl() {
    return replicationUrl;
  }

  public String getReplicationPassword() {
    return replicationPassword;
  }

  public String getReplicationCaCertificate() {
    return replicationCaCertificate;
  }

}
