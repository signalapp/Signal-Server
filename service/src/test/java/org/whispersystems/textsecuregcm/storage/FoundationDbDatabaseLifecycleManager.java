/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import java.io.IOException;

interface FoundationDbDatabaseLifecycleManager {

  void initializeDatabase(final FDB fdb) throws IOException;

  Database getDatabase();

  void closeDatabase();
}
