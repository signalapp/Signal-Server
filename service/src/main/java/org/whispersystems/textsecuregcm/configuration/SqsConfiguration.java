/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

public class SqsConfiguration {
  @NotEmpty
  @JsonProperty
  private String accessKey;

  @NotEmpty
  @JsonProperty
  private String accessSecret;

  @NotEmpty
  @JsonProperty
  private List<String> queueUrls;

  @NotEmpty
  @JsonProperty
  private String region = "us-east-1";

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessSecret() {
    return accessSecret;
  }

  public List<String> getQueueUrls() {
    return queueUrls;
  }

  public String getRegion() {
    return region;
  }
}
