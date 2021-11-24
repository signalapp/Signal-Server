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

public class ProfilesPostgresTest extends ProfilesTest {

  @RegisterExtension
  static PreparedDbExtension ACCOUNTS_POSTGRES_EXTENSION =
      EmbeddedPostgresExtension.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private Profiles profiles;

  @BeforeEach
  void setUp() {
    final FaultTolerantDatabase faultTolerantDatabase = new FaultTolerantDatabase("profilesTest",
        Jdbi.create(ACCOUNTS_POSTGRES_EXTENSION.getTestDatabase()),
        new CircuitBreakerConfiguration());

    profiles = new Profiles(faultTolerantDatabase);

    faultTolerantDatabase.use(jdbi -> jdbi.useHandle(handle -> handle.createUpdate("DELETE FROM profiles").execute()));
  }

  @Override
  protected ProfilesStore getProfilesStore() {
    return profiles;
  }
}
