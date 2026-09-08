/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.FDB;
import java.io.IOException;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.whispersystems.textsecuregcm.storage.foundationdb.FaultTolerantDatabase;

public class FoundationDbClusterExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private FoundationDbDatabaseLifecycleManager[] databaseLifecycleManagers;
  private FaultTolerantDatabase[] databases;

  public FoundationDbClusterExtension(final int numInstances) {
    this.databaseLifecycleManagers = new FoundationDbDatabaseLifecycleManager[numInstances];
    this.databases = new FaultTolerantDatabase[numInstances];
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws IOException {
    if (databaseLifecycleManagers[0] == null) {
      final String serviceContainerNamePrefix = System.getProperty("foundationDb.serviceContainerNamePrefix");

      for (int i = 0; i < databaseLifecycleManagers.length; i++) {
        final FoundationDbDatabaseLifecycleManager databaseLifecycleManager = serviceContainerNamePrefix != null
                ? new ServiceContainerFoundationDbDatabaseLifecycleManager(serviceContainerNamePrefix + i)
                : new TestcontainersFoundationDbDatabaseLifecycleManager();
        databaseLifecycleManager.initializeDatabase(FDB.selectAPIVersion(FoundationDbVersion.getFoundationDbApiVersion()));
        databaseLifecycleManagers[i] = databaseLifecycleManager;
        databases[i] = new FaultTolerantDatabase(databaseLifecycleManager.getDatabase(), String.format("messages-%d", i), null);
      }

    }
  }

  public FaultTolerantDatabase[] getDatabases() {
    return databases;
  }

  @Override
  public void close() throws Throwable {
    if (databaseLifecycleManagers[0] != null) {
      for (final FoundationDbDatabaseLifecycleManager databaseLifecycleManager : databaseLifecycleManagers) {
        databaseLifecycleManager.closeDatabase();
      }
    }
  }
}
