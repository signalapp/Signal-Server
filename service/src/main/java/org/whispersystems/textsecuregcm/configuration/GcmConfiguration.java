/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

public class GcmConfiguration {

  @NotNull
  @JsonProperty
  private long senderId;

  @NotEmpty
  @JsonProperty
  private String apiKey;

  public String getApiKey() {
    return apiKey;
  }

  public long getSenderId() {
    return senderId;
  }

}
