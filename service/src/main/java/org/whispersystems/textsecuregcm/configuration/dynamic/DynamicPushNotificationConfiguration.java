/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicPushNotificationConfiguration {

  @JsonProperty
  private boolean lowUrgencyEnabled = false;

  public boolean isLowUrgencyEnabled() {
    return lowUrgencyEnabled;
  }
}
