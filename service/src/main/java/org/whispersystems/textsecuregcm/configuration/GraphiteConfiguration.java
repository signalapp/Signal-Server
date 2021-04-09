/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GraphiteConfiguration {
  @JsonProperty
  private String host;

  @JsonProperty
  private int port;

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public boolean isEnabled() {
    return host != null && port != 0;
  }
}
