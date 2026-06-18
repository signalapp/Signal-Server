/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.io.IOException;
import org.whispersystems.textsecuregcm.storage.TestcontainersFoundationDbDatabaseLifecycleManager;

@JsonTypeName("local")
public class LocalFoundationDbDatabaseFactory implements FoundationDbDatabaseFactory {

  @JsonIgnore
  private final TestcontainersFoundationDbDatabaseLifecycleManager testcontainersFoundationDbDatabaseLifecycleManager;

  @JsonIgnore
  private Database database;

  private LocalFoundationDbDatabaseFactory() {
    this.testcontainersFoundationDbDatabaseLifecycleManager = new TestcontainersFoundationDbDatabaseLifecycleManager();
  }

  @Override
  public synchronized Database build(final FDB fdb) throws IOException {
    if (database == null) {
      Runtime.getRuntime().addShutdownHook(new Thread(testcontainersFoundationDbDatabaseLifecycleManager::closeDatabase));
      testcontainersFoundationDbDatabaseLifecycleManager.initializeDatabase(fdb);
      database = testcontainersFoundationDbDatabaseLifecycleManager.getDatabase();
    }

    return database;
  }
}
