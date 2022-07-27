/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicMessagePersisterConfiguration {

  @JsonProperty
  private boolean persistenceEnabled = true;

  public boolean isPersistenceEnabled() {
    return persistenceEnabled;
  }
}
