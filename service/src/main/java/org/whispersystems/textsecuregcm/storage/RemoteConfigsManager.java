/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.base.Suppliers;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RemoteConfigsManager {

  private final RemoteConfigs remoteConfigs;

  private final Supplier<List<RemoteConfig>> remoteConfigSupplier;

  public RemoteConfigsManager(RemoteConfigs remoteConfigs) {
    this.remoteConfigs = remoteConfigs;

    remoteConfigSupplier =
        Suppliers.memoizeWithExpiration(remoteConfigs::getAll, 10, TimeUnit.SECONDS);
  }

  public List<RemoteConfig> getAll() {
    return remoteConfigSupplier.get();
  }

  public void set(RemoteConfig config) {
    remoteConfigs.set(config);
  }

  public void delete(String name) {
    remoteConfigs.delete(name);
  }
}
