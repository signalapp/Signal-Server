/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import earth.adi.testcontainers.containers.FoundationDBContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;

class TestcontainersFoundationDbDatabaseLifecycleManager implements FoundationDbDatabaseLifecycleManager {

  private FoundationDBContainer foundationDBContainer;
  private Database database;

  private static final String FOUNDATIONDB_IMAGE_NAME = "foundationdb/foundationdb:" + FoundationDbVersion.getFoundationDbVersion();

  private static final Logger log = LoggerFactory.getLogger(TestcontainersFoundationDbDatabaseLifecycleManager.class);

  @Override
  public void initializeDatabase(final FDB fdb) {
    log.info("Using Testcontainers FoundationDB container: {}", FOUNDATIONDB_IMAGE_NAME);

    foundationDBContainer = new FoundationDBContainer(DockerImageName.parse(FOUNDATIONDB_IMAGE_NAME));
    foundationDBContainer.start();

    database = fdb.open(foundationDBContainer.getClusterFilePath());
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  @Override
  public void closeDatabase() {
    database.close();
    foundationDBContainer.close();
  }
}
