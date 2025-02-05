/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class FakeDynamicConfigurationManager<T> extends DynamicConfigurationManager<T> {

  T staticConfiguration;

  public FakeDynamicConfigurationManager(T staticConfiguration) {
    super(null, (Class<T>) staticConfiguration.getClass());
    this.staticConfiguration = staticConfiguration;
  }

  @Override
  public T getConfiguration() {
    return staticConfiguration;
  }

}
