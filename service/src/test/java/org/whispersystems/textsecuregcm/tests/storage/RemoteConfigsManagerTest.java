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
import org.whispersystems.textsecuregcm.storage.RemoteConfigsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import java.util.HashSet;
import java.util.List;

import io.dropwizard.auth.Auth;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class RemoteConfigsManagerTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private RemoteConfigsManager remoteConfigs;

  @Before
  public void setup() {
    RemoteConfigs remoteConfigs = new RemoteConfigs(new FaultTolerantDatabase("remote_configs-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
    this.remoteConfigs = new RemoteConfigsManager(remoteConfigs, 500);
    this.remoteConfigs.start();
  }

  @Test
  public void testUpdate() throws InterruptedException {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, new HashSet<>() {{
      add(AuthHelper.VALID_UUID);
    }}));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 50, new HashSet<>()));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 75, new HashSet<>()));

    Thread.sleep(501);

    List<RemoteConfig> results = remoteConfigs.getAll();

    assertThat(results.size()).isEqualTo(2);
    assertThat(results.get(0).getName()).isEqualTo("android.stickers");
    assertThat(results.get(0).getPercentage()).isEqualTo(50);
    assertThat(results.get(0).getUuids().size()).isEqualTo(1);
    assertThat(results.get(0).getUuids().contains(AuthHelper.VALID_UUID)).isTrue();

    assertThat(results.get(1).getName()).isEqualTo("ios.stickers");
    assertThat(results.get(1).getPercentage()).isEqualTo(75);
    assertThat(results.get(1).getUuids()).isEmpty();

  }

}
