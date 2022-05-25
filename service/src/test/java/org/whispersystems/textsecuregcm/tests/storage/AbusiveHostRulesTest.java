/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class AbusiveHostRulesTest {

  @RegisterExtension
  private static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();
  private AbusiveHostRules abusiveHostRules;
  private DynamicConfigurationManager<DynamicConfiguration> mockDynamicConfigManager;

  @BeforeEach
  void setup() throws JsonProcessingException {
    @SuppressWarnings("unchecked")
    DynamicConfigurationManager<DynamicConfiguration> m = mock(DynamicConfigurationManager.class);
    this.mockDynamicConfigManager = m;
    when(mockDynamicConfigManager.getConfiguration()).thenReturn(generateConfig(Duration.ofHours(1)));
    this.abusiveHostRules = new AbusiveHostRules(REDIS_CLUSTER_EXTENSION.getRedisCluster(), mockDynamicConfigManager);
  }

  DynamicConfiguration generateConfig(Duration expireDuration) throws JsonProcessingException {
    final String configString = String.format("""
        captcha:
          scoreFloor: 1.0
        abusiveHostRules:
          expirationTime: %s
         """, expireDuration);
    return DynamicConfigurationManager
        .parseConfiguration(configString, DynamicConfiguration.class)
        .orElseThrow();
  }

  @Test
  void testBlockedHost() {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection ->
        connection.sync().set(AbusiveHostRules.prefix("192.168.1.1"), "1"));
    assertThat(abusiveHostRules.isBlocked("192.168.1.1")).isTrue();
  }

  @Test
  void testUnblocked() {
    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(connection ->
        connection.sync().set(AbusiveHostRules.prefix("192.168.1.1"), "1"));
    assertThat(abusiveHostRules.isBlocked("172.17.1.1")).isFalse();
  }

  @Test
  void testInsertBlocked() {
    abusiveHostRules.setBlockedHost("172.17.0.1");
    assertThat(abusiveHostRules.isBlocked("172.17.0.1")).isTrue();
    abusiveHostRules.setBlockedHost("172.17.0.1");
    assertThat(abusiveHostRules.isBlocked("172.17.0.1")).isTrue();
  }

  @Test
 void testExpiration() throws Exception {
    when(mockDynamicConfigManager.getConfiguration()).thenReturn(generateConfig(Duration.ofSeconds(1)));
    abusiveHostRules.setBlockedHost("192.168.1.1");
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      while (true) {
        if (!abusiveHostRules.isBlocked("192.168.1.1")) {
          break;
        }
      }
    });
  }

}
