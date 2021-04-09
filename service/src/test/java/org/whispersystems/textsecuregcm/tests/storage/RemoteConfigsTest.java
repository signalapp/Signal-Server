/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteConfigsTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private RemoteConfigs remoteConfigs;

  @Before
  public void setup() {
    this.remoteConfigs = new RemoteConfigs(new FaultTolerantDatabase("remote_configs-test", Jdbi.create(db.getTestDatabase()), new CircuitBreakerConfiguration()));
  }

  @Test
  public void testStore() {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, Set.of(AuthHelper.VALID_UUID, AuthHelper.VALID_UUID_TWO), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("value.sometimes", 25, Set.of(AuthHelper.VALID_UUID_TWO), "default", "custom", null));

    List<RemoteConfig> configs = remoteConfigs.getAll();

    assertThat(configs).hasSize(2);

    assertThat(configs.get(0).getName()).isEqualTo("android.stickers");
    assertThat(configs.get(0).getValue()).isEqualTo("TRUE");
    assertThat(configs.get(0).getDefaultValue()).isEqualTo("FALSE");
    assertThat(configs.get(0).getPercentage()).isEqualTo(50);
    assertThat(configs.get(0).getUuids()).hasSize(2);
    assertThat(configs.get(0).getUuids()).contains(AuthHelper.VALID_UUID);
    assertThat(configs.get(0).getUuids()).contains(AuthHelper.VALID_UUID_TWO);
    assertThat(configs.get(0).getUuids()).doesNotContain(AuthHelper.INVALID_UUID);

    assertThat(configs.get(1).getName()).isEqualTo("value.sometimes");
    assertThat(configs.get(1).getValue()).isEqualTo("custom");
    assertThat(configs.get(1).getDefaultValue()).isEqualTo("default");
    assertThat(configs.get(1).getPercentage()).isEqualTo(25);
    assertThat(configs.get(1).getUuids()).hasSize(1);
    assertThat(configs.get(1).getUuids()).contains(AuthHelper.VALID_UUID_TWO);
    assertThat(configs.get(1).getUuids()).doesNotContain(AuthHelper.VALID_UUID);
    assertThat(configs.get(1).getUuids()).doesNotContain(AuthHelper.INVALID_UUID);
  }

  @Test
  public void testUpdate() {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, Set.of(), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("value.sometimes", 22, Set.of(), "def", "!", null));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 50, Set.of(AuthHelper.DISABLED_UUID), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 75, Set.of(), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("value.sometimes", 77, Set.of(), "hey", "wut", null));

    List<RemoteConfig> configs = remoteConfigs.getAll();

    assertThat(configs).hasSize(3);

    assertThat(configs.get(0).getName()).isEqualTo("android.stickers");
    assertThat(configs.get(0).getPercentage()).isEqualTo(50);
    assertThat(configs.get(0).getUuids()).isEmpty();
    assertThat(configs.get(0).getDefaultValue()).isEqualTo("FALSE");
    assertThat(configs.get(0).getValue()).isEqualTo("TRUE");

    assertThat(configs.get(1).getName()).isEqualTo("ios.stickers");
    assertThat(configs.get(1).getPercentage()).isEqualTo(75);
    assertThat(configs.get(1).getUuids()).isEmpty();
    assertThat(configs.get(1).getDefaultValue()).isEqualTo("FALSE");
    assertThat(configs.get(1).getValue()).isEqualTo("TRUE");

    assertThat(configs.get(2).getName()).isEqualTo("value.sometimes");
    assertThat(configs.get(2).getPercentage()).isEqualTo(77);
    assertThat(configs.get(2).getUuids()).isEmpty();
    assertThat(configs.get(2).getDefaultValue()).isEqualTo("hey");
    assertThat(configs.get(2).getValue()).isEqualTo("wut");
  }

  @Test
  public void testDelete() {
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, Set.of(AuthHelper.VALID_UUID), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 50, Set.of(), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 75, Set.of(), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("value.always", 100, Set.of(), "never", "always", null));
    remoteConfigs.delete("android.stickers");

    List<RemoteConfig> configs = remoteConfigs.getAll();

    assertThat(configs).hasSize(2);

    assertThat(configs.get(0).getName()).isEqualTo("ios.stickers");
    assertThat(configs.get(0).getPercentage()).isEqualTo(75);
    assertThat(configs.get(0).getDefaultValue()).isEqualTo("FALSE");
    assertThat(configs.get(0).getValue()).isEqualTo("TRUE");

    assertThat(configs.get(1).getName()).isEqualTo("value.always");
    assertThat(configs.get(1).getPercentage()).isEqualTo(100);
    assertThat(configs.get(1).getValue()).isEqualTo("always");
    assertThat(configs.get(1).getDefaultValue()).isEqualTo("never");
  }
}
