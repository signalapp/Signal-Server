/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class RecaptchaConfiguration {

  @JsonProperty
  @NotEmpty
  private String secret;

  public String getSecret() {
    return secret;
  }

}
