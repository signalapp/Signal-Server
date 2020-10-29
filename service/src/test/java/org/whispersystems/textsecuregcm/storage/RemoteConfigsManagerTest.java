/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

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
    remoteConfigs.set(new RemoteConfig("android.stickers", 50, Set.of(AuthHelper.VALID_UUID), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("value.sometimes", 50, Set.of(), "bar", "baz", null));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 50, Set.of(), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("ios.stickers", 75, Set.of(), "FALSE", "TRUE", null));
    remoteConfigs.set(new RemoteConfig("value.sometimes", 25, Set.of(AuthHelper.VALID_UUID), "abc", "def", null));

    remoteConfigs.waitForCacheRefresh();

    List<RemoteConfig> results = remoteConfigs.getAll();

    assertThat(results.size()).isEqualTo(3);

    assertThat(results.get(0).getName()).isEqualTo("android.stickers");
    assertThat(results.get(0).getPercentage()).isEqualTo(50);
    assertThat(results.get(0).getUuids().size()).isEqualTo(1);
    assertThat(results.get(0).getUuids().contains(AuthHelper.VALID_UUID)).isTrue();

    assertThat(results.get(1).getName()).isEqualTo("ios.stickers");
    assertThat(results.get(1).getPercentage()).isEqualTo(75);
    assertThat(results.get(1).getUuids()).isEmpty();

    assertThat(results.get(2).getName()).isEqualTo("value.sometimes");
    assertThat(results.get(2).getUuids()).hasSize(1);
    assertThat(results.get(2).getUuids()).contains(AuthHelper.VALID_UUID);
    assertThat(results.get(2).getPercentage()).isEqualTo(25);
    assertThat(results.get(2).getDefaultValue()).isEqualTo("abc");
    assertThat(results.get(2).getValue()).isEqualTo("def");

  }

}
