/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle of a database connected to a FoundationDB instance running as an external service container.
 */
class ServiceContainerFoundationDbDatabaseLifecycleManager implements FoundationDbDatabaseLifecycleManager {

  private final String foundationDbServiceContainerName;

  private Database database;

  private static final Logger log = LoggerFactory.getLogger(ServiceContainerFoundationDbDatabaseLifecycleManager.class);

  ServiceContainerFoundationDbDatabaseLifecycleManager(final String foundationDbServiceContainerName) {
    log.info("Using FoundationDB service container: {}", foundationDbServiceContainerName);
    this.foundationDbServiceContainerName = foundationDbServiceContainerName;
  }

  @Override
  public void initializeDatabase(final FDB fdb) throws IOException {
    final File clusterFile = File.createTempFile("fdb.cluster", "");
    clusterFile.deleteOnExit();

    try (final FileWriter fileWriter = new FileWriter(clusterFile)) {
      fileWriter.write(String.format("docker:docker@%s:4500", foundationDbServiceContainerName));
    }

    database = fdb.open(clusterFile.getAbsolutePath());
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  @Override
  public void closeDatabase() {
    database.close();
  }
}
