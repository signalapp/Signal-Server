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
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;

import io.dropwizard.auth.Auth;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class RemoteConfigsTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private RemoteConfigs remoteConfigs;

  @Before
  public void setup() {
    this.remoteConfigs = new RemoteConfigs(new FaultTolerantDatabase("remote_configs-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  }

  @Test
  public void testStore() throws SQLException {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, new HashSet<>() {{
      add(AuthHelper.VALID_UUID);
      add(AuthHelper.VALID_UUID_TWO);
    }}));

    List<RemoteConfig> configs = remoteConfigs.getAll();

    assertThat(configs.size()).isEqualTo(1);
    assertThat(configs.get(0).getName()).isEqualTo("android.stickers");
    assertThat(configs.get(0).getPercentage()).isEqualTo(50);
    assertThat(configs.get(0).getUuids().size()).isEqualTo(2);
    assertThat(configs.get(0).getUuids().contains(AuthHelper.VALID_UUID)).isTrue();
    assertThat(configs.get(0).getUuids().contains(AuthHelper.VALID_UUID_TWO)).isTrue();
    assertThat(configs.get(0).getUuids().contains(AuthHelper.INVALID_UUID)).isFalse();
  }

  @Test
  public void testUpdate() throws SQLException {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, new HashSet<>()));

    remoteConfigs.set(new RemoteConfig("ios.stickers", 50, new HashSet<>() {{
      add(AuthHelper.DISABLED_UUID);
    }}));

    remoteConfigs.set(new RemoteConfig("ios.stickers", 75, new HashSet<>()));

    List<RemoteConfig> configs = remoteConfigs.getAll();

    assertThat(configs.size()).isEqualTo(2);
    assertThat(configs.get(0).getName()).isEqualTo("android.stickers");
    assertThat(configs.get(0).getPercentage()).isEqualTo(50);
    assertThat(configs.get(0).getUuids().size()).isEqualTo(0);

    assertThat(configs.get(1).getName()).isEqualTo("ios.stickers");
    assertThat(configs.get(1).getPercentage()).isEqualTo(75);
    assertThat(configs.get(1).getUuids().size()).isEqualTo(0);
  }

  @Test
  public void testDelete() {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, new HashSet<>() {{
      add(AuthHelper.VALID_UUID);
    }}));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 50, new HashSet<>()));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 75, new HashSet<>()));
    remoteConfigs.delete("android.stickers");

    List<RemoteConfig> configs = remoteConfigs.getAll();

    assertThat(configs.size()).isEqualTo(1);
    assertThat(configs.get(0).getName()).isEqualTo("ios.stickers");
    assertThat(configs.get(0).getPercentage()).isEqualTo(75);
  }


}
