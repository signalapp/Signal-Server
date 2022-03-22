/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

public class TurnToken {

  @JsonProperty
  private String username;

  @JsonProperty
  private String password;

  @JsonProperty
  private List<String> urls;

  public TurnToken(String username, String password, List<String> urls) {
    this.username = username;
    this.password = password;
    this.urls     = urls;
  }

  @VisibleForTesting
  List<String> getUrls() {
    return urls;
  }
}
