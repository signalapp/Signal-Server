/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import java.io.IOException;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class FoundationDbExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

  private static FoundationDbDatabaseLifecycleManager databaseLifecycleManager;

  @Override
  public void beforeAll(final ExtensionContext context) throws IOException {
    if (databaseLifecycleManager == null) {
      final String serviceContainerName = System.getProperty("foundationDb.serviceContainerName");

      databaseLifecycleManager = serviceContainerName != null
          ? new ServiceContainerFoundationDbDatabaseLifecycleManager(serviceContainerName)
          : new TestcontainersFoundationDbDatabaseLifecycleManager();

      databaseLifecycleManager.initializeDatabase(FDB.selectAPIVersion(FoundationDbVersion.getFoundationDbApiVersion()));

      context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).put(getClass().getName(), this);
    }
  }

  public Database getDatabase() {
    return databaseLifecycleManager.getDatabase();
  }

  @Override
  public void close() throws Throwable {
    if (databaseLifecycleManager != null) {
      databaseLifecycleManager.closeDatabase();
    }
  }
}
