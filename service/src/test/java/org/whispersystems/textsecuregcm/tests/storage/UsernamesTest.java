package org.whispersystems.textsecuregcm.tests.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Usernames;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertFalse;

public class UsernamesTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private Usernames usernames;

  @Before
  public void setupAccountsDao() {
    FaultTolerantDatabase faultTolerantDatabase = new FaultTolerantDatabase("usernamesTest",
                                                                            Jdbi.create(db.getTestDatabase()),
                                                                            new CircuitBreakerConfiguration());

    this.usernames = new Usernames(faultTolerantDatabase);
  }

  @Test
  public void testPut() throws SQLException, IOException {
    UUID   uuid     = UUID.randomUUID();
    String username = "myusername";

    assertTrue(usernames.put(uuid, username));

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM usernames WHERE uuid = ?");
    verifyStoredState(statement, uuid, username);
  }

  @Test
  public void testPutChange() throws SQLException, IOException {
    UUID uuid = UUID.randomUUID();
    String firstUsername = "myfirstusername";
    String secondUsername = "mysecondusername";

    assertTrue(usernames.put(uuid, firstUsername));

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM usernames WHERE uuid = ?");
    verifyStoredState(statement, uuid, firstUsername);

    assertTrue(usernames.put(uuid, secondUsername));

    verifyStoredState(statement, uuid, secondUsername);
  }

  @Test
  public void testPutConflict() throws SQLException {
    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    String username = "myfirstusername";

    assertTrue(usernames.put(firstUuid, username));
    assertFalse(usernames.put(secondUuid, username));

    PreparedStatement statement = db.getTestDatabase().getConnection().prepareStatement("SELECT * FROM usernames WHERE username = ?");
    statement.setString(1, username);

    ResultSet resultSet = statement.executeQuery();

    assertTrue(resultSet.next());
    assertThat(resultSet.getString("uuid")).isEqualTo(firstUuid.toString());
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void testGetByUuid() {
    UUID   uuid     = UUID.randomUUID();
    String username = "myusername";

    assertTrue(usernames.put(uuid, username));

    Optional<String> retrieved = usernames.get(uuid);

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get()).isEqualTo(username);
  }

  @Test
  public void testGetByUuidMissing() {
    Optional<String> retrieved = usernames.get(UUID.randomUUID());
    assertFalse(retrieved.isPresent());
  }

  @Test
  public void testGetByUsername() {
    UUID   uuid     = UUID.randomUUID();
    String username = "myusername";

    assertTrue(usernames.put(uuid, username));

    Optional<UUID> retrieved = usernames.get(username);

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get()).isEqualTo(uuid);
  }

  @Test
  public void testGetByUsernameMissing() {
    Optional<UUID> retrieved = usernames.get("myusername");

    assertFalse(retrieved.isPresent());
  }


  @Test
  public void testDelete() {
    UUID   uuid     = UUID.randomUUID();
    String username = "myusername";

    assertTrue(usernames.put(uuid, username));

    Optional<UUID> retrieved = usernames.get(username);

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get()).isEqualTo(uuid);

    usernames.delete(uuid);

    assertThat(usernames.get(uuid).isPresent()).isFalse();
  }

  private void verifyStoredState(PreparedStatement statement, UUID uuid, String expectedUsername)
      throws SQLException, IOException
  {
    statement.setObject(1, uuid);

    ResultSet resultSet = statement.executeQuery();

    if (resultSet.next()) {
      String data = resultSet.getString("username");
      assertThat(data).isNotEmpty();
      assertThat(data).isEqualTo(expectedUsername);
    } else {
      throw new AssertionError("No data");
    }

    assertThat(resultSet.next()).isFalse();
  }


}
