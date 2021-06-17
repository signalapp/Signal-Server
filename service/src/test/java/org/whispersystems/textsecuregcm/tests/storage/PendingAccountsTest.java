/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.PendingAccounts;

class PendingAccountsTest {

  @RegisterExtension
  static PreparedDbExtension db = EmbeddedPostgresExtension.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private PendingAccounts pendingAccounts;

  @BeforeEach
  void setupAccountsDao() {
    this.pendingAccounts = new PendingAccounts(new FaultTolerantDatabase("pending_accounts-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  }

  @Test
  void testStore() throws SQLException {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("1234", 1111, null, null));

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM pending_accounts WHERE number = ?");
    statement.setString(1, "+14151112222");

    ResultSet resultSet = statement.executeQuery();

    if (resultSet.next()) {
      assertThat(resultSet.getString("verification_code")).isEqualTo("1234");
      assertThat(resultSet.getLong("timestamp")).isEqualTo(1111);
      assertThat(resultSet.getString("push_code")).isNull();
    } else {
      throw new AssertionError("no results");
    }

    assertThat(resultSet.next()).isFalse();
  }

  @Test
  void testStoreWithPushChallenge() throws SQLException {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode(null, 1111,  "112233", null));

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM pending_accounts WHERE number = ?");
    statement.setString(1, "+14151112222");

    ResultSet resultSet = statement.executeQuery();

    if (resultSet.next()) {
      assertThat(resultSet.getString("verification_code")).isNull();
      assertThat(resultSet.getLong("timestamp")).isEqualTo(1111);
      assertThat(resultSet.getString("push_code")).isEqualTo("112233");
    } else {
      throw new AssertionError("no results");
    }

    assertThat(resultSet.next()).isFalse();
  }

  @Test
  void testStoreWithTwilioVerificationSid() throws SQLException {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode(null, 1111, null, "id1"));

    PreparedStatement statement = db.getTestDatabase().getConnection()
        .prepareStatement("SELECT * FROM pending_accounts WHERE number = ?");
    statement.setString(1, "+14151112222");

    ResultSet resultSet = statement.executeQuery();

    if (resultSet.next()) {
      assertThat(resultSet.getString("verification_code")).isNull();
      assertThat(resultSet.getLong("timestamp")).isEqualTo(1111);
      assertThat(resultSet.getString("push_code")).isNull();
      assertThat(resultSet.getString("twilio_verification_sid")).isEqualTo("id1");
    } else {
      throw new AssertionError("no results");
    }

    assertThat(resultSet.next()).isFalse();
  }

  @Test
  void testRetrieve() throws Exception {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, null, null));
    pendingAccounts.insert("+14151113333", new StoredVerificationCode("1212", 5555, null, null));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4321");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(2222);

    Optional<StoredVerificationCode> missingCode = pendingAccounts.findForNumber("+11111111111");
    assertThat(missingCode.isPresent()).isFalse();
  }

  @Test
  void testRetrieveWithPushChallenge() throws Exception {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, "bar", null));
    pendingAccounts.insert("+14151113333", new StoredVerificationCode("1212", 5555, "bang", null));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4321");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(2222);
    assertThat(verificationCode.get().getPushCode()).isEqualTo("bar");

    Optional<StoredVerificationCode> missingCode = pendingAccounts.findForNumber("+11111111111");
    assertThat(missingCode.isPresent()).isFalse();
  }

  @Test
  void testRetrieveWithTwilioVerificationSid() throws Exception {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, "bar", "id1"));
    pendingAccounts.insert("+14151113333", new StoredVerificationCode("1212", 5555, "bang", "id2"));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode).isPresent();
    assertThat(verificationCode.get().getCode()).isEqualTo("4321");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(2222);
    assertThat(verificationCode.get().getPushCode()).isEqualTo("bar");
    assertThat(verificationCode.get().getTwilioVerificationSid()).contains("id1");

    Optional<StoredVerificationCode> missingCode = pendingAccounts.findForNumber("+11111111111");
    assertThat(missingCode).isNotPresent();
  }

  @Test
  void testOverwrite() throws Exception {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, null, null));
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4444", 3333, null, null));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4444");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(3333);
  }

  @Test
  void testOverwriteWithPushToken() throws Exception {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, "bar", null));
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4444", 3333, "bang", null));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4444");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(3333);
    assertThat(verificationCode.get().getPushCode()).isEqualTo("bang");
  }

  @Test
  void testOverwriteWithTwilioVerificationSid() throws Exception {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, "bar", "id1"));
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4444", 3333, "bang", "id2"));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4444");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(3333);
    assertThat(verificationCode.get().getPushCode()).isEqualTo("bang");
    assertThat(verificationCode.get().getTwilioVerificationSid()).contains("id2");
  }

  @Test
  void testVacuum() {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, null, null));
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4444", 3333, null, null));
    pendingAccounts.vacuum();

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4444");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(3333);
  }

  @Test
  void testRemove() {
    pendingAccounts.insert("+14151112222", new StoredVerificationCode("4321", 2222, "bar", null));
    pendingAccounts.insert("+14151113333", new StoredVerificationCode("1212", 5555, null, null));

    Optional<StoredVerificationCode> verificationCode = pendingAccounts.findForNumber("+14151112222");

    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("4321");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(2222);

    pendingAccounts.remove("+14151112222");

    verificationCode = pendingAccounts.findForNumber("+14151112222");
    assertThat(verificationCode.isPresent()).isFalse();

    verificationCode = pendingAccounts.findForNumber("+14151113333");
    assertThat(verificationCode.isPresent()).isTrue();
    assertThat(verificationCode.get().getCode()).isEqualTo("1212");
    assertThat(verificationCode.get().getTimestamp()).isEqualTo(5555);
    assertThat(verificationCode.get().getPushCode()).isNull();
  }
}
