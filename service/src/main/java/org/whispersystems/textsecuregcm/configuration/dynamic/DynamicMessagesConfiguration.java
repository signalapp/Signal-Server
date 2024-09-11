/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

public record DynamicMessagesConfiguration(boolean storeSharedMrmData, boolean mrmViewExperimentEnabled) {

  public DynamicMessagesConfiguration() {
    this(false, false);
  }
}
