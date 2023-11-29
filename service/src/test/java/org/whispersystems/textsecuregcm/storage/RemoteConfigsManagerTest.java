/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

class RemoteConfigsManagerTest {

  private RemoteConfigs remoteConfigs;
  private RemoteConfigsManager remoteConfigsManager;

  @BeforeEach
  void setup() {
    this.remoteConfigs = mock(RemoteConfigs.class);
    this.remoteConfigsManager = new RemoteConfigsManager(remoteConfigs);
  }

  @Test
  void testGetAll() {
    remoteConfigsManager.getAll();
    remoteConfigsManager.getAll();

    // A memoized supplier should prevent multiple calls to the underlying data source
    verify(remoteConfigs, times(1)).getAll();
  }
}
