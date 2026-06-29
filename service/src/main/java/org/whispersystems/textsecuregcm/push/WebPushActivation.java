/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

public record WebPushActivation(
                                @JsonProperty
                                boolean activated,
                                @JsonProperty
                                String activationToken) {
  public static WebPushActivation newToken() {
    return new WebPushActivation(
      false,
      UUID.randomUUID().toString()
    );
  }
}
