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
import java.sql.SQLException;
import java.sql.Statement;

class PendingAccountsTest extends VerificationCodeStoreTest {

  @RegisterExtension
  public static PreparedDbExtension db = EmbeddedPostgresExtension.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private PendingAccounts pendingAccounts;

  @BeforeEach
  void setupAccountsDao() throws SQLException {
    this.pendingAccounts = new PendingAccounts(new FaultTolerantDatabase("pending_accounts-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));

    try (final Statement deleteStatement = db.getTestDatabase().getConnection().createStatement()) {
      deleteStatement.execute("DELETE FROM pending_accounts");
    }
  }

  @Override
  protected VerificationCodeStore getVerificationCodeStore() {
    return pendingAccounts;
  }

  @Override
  protected boolean expectNullPushCode() {
    return false;
  }

  @Override
  protected boolean expectEmptyTwilioSid() {
    return false;
  }
}
