/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;

class ChangeNumberWaitingPeriodManagerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static final Duration WAITING_PERIOD = Duration.ofDays(7);

  private ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager;

  @BeforeEach
  void setUp() {
    changeNumberWaitingPeriodManager = new ChangeNumberWaitingPeriodManager(
        REDIS_CLUSTER_EXTENSION.getRedisCluster(), WAITING_PERIOD);
  }

  @Test
  void testNewAccount() throws Exception {
    final UUID aci = UUID.randomUUID();

    assertTrue(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci).isEmpty());

    changeNumberWaitingPeriodManager.handleAccountCreated(aci, Instant.now())
        .get(5, TimeUnit.SECONDS);

    assertTrue(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci).isPresent());
  }

  @Test
  void testOldAccount() throws Exception {
    final UUID aci = UUID.randomUUID();

    changeNumberWaitingPeriodManager.handleAccountCreated(aci,
        Instant.now().minus(WAITING_PERIOD).minus(Duration.ofHours(1)))
        .get(5, TimeUnit.SECONDS);

    assertTrue(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci).isEmpty());
  }

  @Test
  void testNoTtlException() throws Exception {
    final UUID aci = UUID.randomUUID();

    changeNumberWaitingPeriodManager.handleAccountCreated(aci, Instant.now()).get(5, TimeUnit.SECONDS);

    REDIS_CLUSTER_EXTENSION.getRedisCluster().useCluster(conn ->
        conn.sync().persist(ChangeNumberWaitingPeriodManager.key(aci)));

    assertThrows(RuntimeException.class, () -> changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(aci),
        "This is an impossible scenario, and it should throw an exception");
  }
}
