/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.zonky.test.db.postgres.embedded.LiquibasePreparer;
import io.zonky.test.db.postgres.junit5.EmbeddedPostgresExtension;
import io.zonky.test.db.postgres.junit5.PreparedDbExtension;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicAbusiveHostRulesMigrationConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;

class AbusiveHostRulesTest {

  @RegisterExtension
  PreparedDbExtension db = EmbeddedPostgresExtension.preparedDatabase(
      LiquibasePreparer.forClasspathLocation("abusedb.xml"));

  @RegisterExtension
  PreparedDbExtension newDb = EmbeddedPostgresExtension.preparedDatabase(
      LiquibasePreparer.forClasspathLocation("abusedb.xml"));

  private AbusiveHostRules abusiveHostRules;

  @BeforeEach
  void setup() {
    //noinspection unchecked
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
        DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getAbusiveHostRulesMigrationConfiguration()).thenReturn(
        new DynamicAbusiveHostRulesMigrationConfiguration());

    this.abusiveHostRules = new AbusiveHostRules(
        new FaultTolerantDatabase("abusive_hosts-test", Jdbi.create(db.getTestDatabase()),
            new CircuitBreakerConfiguration()),
        new FaultTolerantDatabase("abusive_hosts-test", Jdbi.create(newDb.getTestDatabase()),
            new CircuitBreakerConfiguration()),
        dynamicConfigurationManager);
  }

  @Test
  void testBlockedHost() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection()
        .prepareStatement("INSERT INTO abusive_host_rules (host, blocked) VALUES (?::INET, ?)");
    statement.setString(1, "192.168.1.1");
    statement.setInt(2, 1);
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("192.168.1.1");
    assertThat(rules.size()).isEqualTo(1);
    assertThat(rules.get(0).regions().isEmpty()).isTrue();
    assertThat(rules.get(0).host()).isEqualTo("192.168.1.1");
    assertThat(rules.get(0).blocked()).isTrue();
  }

  @Test
  void testBlockedCidr() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection()
        .prepareStatement("INSERT INTO abusive_host_rules (host, blocked) VALUES (?::INET, ?)");
    statement.setString(1, "192.168.1.0/24");
    statement.setInt(2, 1);
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("192.168.1.1");
    assertThat(rules.size()).isEqualTo(1);
    assertThat(rules.get(0).regions().isEmpty()).isTrue();
    assertThat(rules.get(0).host()).isEqualTo("192.168.1.0/24");
    assertThat(rules.get(0).blocked()).isTrue();
  }

  @Test
  void testUnblocked() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection()
        .prepareStatement("INSERT INTO abusive_host_rules (host, blocked) VALUES (?::INET, ?)");
    statement.setString(1, "192.168.1.0/24");
    statement.setInt(2, 1);
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("172.17.1.1");
    assertThat(rules.isEmpty()).isTrue();
  }

  @Test
  void testRestricted() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection()
        .prepareStatement("INSERT INTO abusive_host_rules (host, blocked, regions) VALUES (?::INET, ?, ?)");
    statement.setString(1, "192.168.1.0/24");
    statement.setInt(2, 0);
    statement.setString(3, "+1,+49");
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("192.168.1.100");
    assertThat(rules.size()).isEqualTo(1);
    assertThat(rules.get(0).blocked()).isFalse();
    assertThat(rules.get(0).regions()).isEqualTo(Arrays.asList("+1", "+49"));
  }

  @Test
  void testInsertBlocked() throws Exception {
    abusiveHostRules.setBlockedHost("172.17.0.1", "Testing one two");

    PreparedStatement statement = db.getTestDatabase().getConnection()
        .prepareStatement("SELECT * from abusive_host_rules WHERE host = ?::inet");
    statement.setString(1, "172.17.0.1");

    ResultSet resultSet = statement.executeQuery();

    assertThat(resultSet.next()).isTrue();

    assertThat(resultSet.getInt("blocked")).isEqualTo(1);
    assertThat(resultSet.getString("regions")).isNullOrEmpty();
    assertThat(resultSet.getString("notes")).isEqualTo("Testing one two");

    abusiveHostRules.setBlockedHost("172.17.0.1", "Different notes");

    statement = db.getTestDatabase().getConnection()
        .prepareStatement("SELECT * from abusive_host_rules WHERE host = ?::inet");
    statement.setString(1, "172.17.0.1");

    resultSet = statement.executeQuery();

    assertThat(resultSet.next()).isTrue();

    assertThat(resultSet.getInt("blocked")).isEqualTo(1);
    assertThat(resultSet.getString("regions")).isNullOrEmpty();
    assertThat(resultSet.getString("notes")).isEqualTo("Testing one two");
  }

  @Test
  void testMigrate() throws Exception {
    final int rules = 20;
    for (int i = 1; i <= rules; i++) {
      abusiveHostRules.setBlockedHost("172.17.0." + i, "Testing one two " + i);
    }

    PreparedStatement statement = newDb.getTestDatabase().getConnection()
        .prepareStatement("SELECT * from abusive_host_rules");

    assertThat(queryResultSize(statement.executeQuery())).isEqualTo(0);

    abusiveHostRules.forEachInOldDatabase((rule, host) -> abusiveHostRules.migrateAbusiveHostRule(rule, host), 5);

    assertThat(queryResultSize(statement.executeQuery())).isEqualTo(rules);
  }

  private int queryResultSize(ResultSet resultSet) throws SQLException {
    int migrated = 0;
    while (resultSet.next()) {
      migrated++;
    }

    return migrated;
  }

}
