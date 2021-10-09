/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class DirectoryV2Configuration {

  @JsonProperty
  @NotNull
  @Valid
  private DirectoryV2ClientConfiguration client;

  public DirectoryV2ClientConfiguration getDirectoryV2ClientConfiguration() {
    return client;
  }
}
