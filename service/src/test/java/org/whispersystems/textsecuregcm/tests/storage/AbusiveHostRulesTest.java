package org.whispersystems.textsecuregcm.tests.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AbusiveHostRulesTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("abusedb.xml"));

  private AbusiveHostRules abusiveHostRules;

  @Before
  public void setup() {
    this.abusiveHostRules = new AbusiveHostRules(new FaultTolerantDatabase("abusive_hosts-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  }

  @Test
  public void testBlockedHost() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("INSERT INTO abusive_host_rules (host, blocked) VALUES (?::INET, ?)");
    statement.setString(1, "192.168.1.1");
    statement.setInt(2, 1);
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("192.168.1.1");
    assertThat(rules.size()).isEqualTo(1);
    assertThat(rules.get(0).getRegions().isEmpty()).isTrue();
    assertThat(rules.get(0).getHost()).isEqualTo("192.168.1.1");
    assertThat(rules.get(0).isBlocked()).isTrue();
  }

  @Test
  public void testBlockedCidr() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("INSERT INTO abusive_host_rules (host, blocked) VALUES (?::INET, ?)");
    statement.setString(1, "192.168.1.0/24");
    statement.setInt(2, 1);
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("192.168.1.1");
    assertThat(rules.size()).isEqualTo(1);
    assertThat(rules.get(0).getRegions().isEmpty()).isTrue();
    assertThat(rules.get(0).getHost()).isEqualTo("192.168.1.0/24");
    assertThat(rules.get(0).isBlocked()).isTrue();
  }

  @Test
  public void testUnblocked() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("INSERT INTO abusive_host_rules (host, blocked) VALUES (?::INET, ?)");
    statement.setString(1, "192.168.1.0/24");
    statement.setInt(2, 1);
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("172.17.1.1");
    assertThat(rules.isEmpty()).isTrue();
  }

  @Test
  public void testRestricted() throws SQLException {
    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("INSERT INTO abusive_host_rules (host, blocked, regions) VALUES (?::INET, ?, ?)");
    statement.setString(1, "192.168.1.0/24");
    statement.setInt(2, 0);
    statement.setString(3, "+1,+49");
    statement.execute();

    List<AbusiveHostRule> rules = abusiveHostRules.getAbusiveHostRulesFor("192.168.1.100");
    assertThat(rules.size()).isEqualTo(1);
    assertThat(rules.get(0).isBlocked()).isFalse();
    assertThat(rules.get(0).getRegions()).isEqualTo(Arrays.asList("+1", "+49"));
  }

  @Test
  public void testInsertBlocked() throws Exception {
    abusiveHostRules.setBlockedHost("172.17.0.1", "Testing one two");

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * from abusive_host_rules WHERE host = ?::inet");
    statement.setString(1, "172.17.0.1");

    ResultSet resultSet = statement.executeQuery();

    assertThat(resultSet.next()).isTrue();

    assertThat(resultSet.getInt("blocked")).isEqualTo(1);
    assertThat(resultSet.getString("regions")).isNullOrEmpty();
    assertThat(resultSet.getString("notes")).isEqualTo("Testing one two");

    abusiveHostRules.setBlockedHost("172.17.0.1", "Different notes");


    statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * from abusive_host_rules WHERE host = ?::inet");
    statement.setString(1, "172.17.0.1");

    resultSet = statement.executeQuery();

    assertThat(resultSet.next()).isTrue();

    assertThat(resultSet.getInt("blocked")).isEqualTo(1);
    assertThat(resultSet.getString("regions")).isNullOrEmpty();
    assertThat(resultSet.getString("notes")).isEqualTo("Testing one two");
  }

}
