/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;

public class RemoteConfigsPostgresTest extends RemoteConfigsTest {

  @RegisterExtension
  static PreparedDbExtension ACCOUNTS_POSTGRES_EXTENSION =
      EmbeddedPostgresExtension.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private RemoteConfigs remoteConfigs;

  @BeforeEach
  void setUp() {
    final FaultTolerantDatabase remoteConfigDatabase = new FaultTolerantDatabase("remote_configs-test", Jdbi.create(ACCOUNTS_POSTGRES_EXTENSION.getTestDatabase()), new CircuitBreakerConfiguration());

    remoteConfigDatabase.use(jdbi -> jdbi.useHandle(handle ->
        handle.createUpdate("DELETE FROM remote_config").execute()));

    this.remoteConfigs = new RemoteConfigs(remoteConfigDatabase);
  }

  @Override
  protected RemoteConfigStore getRemoteConfigStore() {
    return remoteConfigs;
  }
}
