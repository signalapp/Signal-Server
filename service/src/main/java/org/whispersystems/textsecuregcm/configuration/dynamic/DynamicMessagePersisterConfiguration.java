/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicMessagePersisterConfiguration {

  @JsonProperty
  private boolean serverPersistenceEnabled = true;

  @JsonProperty
  private boolean dedicatedProcessEnabled = false;

  public boolean isServerPersistenceEnabled() {
    return serverPersistenceEnabled;
  }

  public boolean isDedicatedProcessEnabled() {
    return dedicatedProcessEnabled;
  }
}
