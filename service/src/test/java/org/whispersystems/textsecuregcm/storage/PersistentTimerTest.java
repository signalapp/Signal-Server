/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.util.TestClock;

class PersistentTimerTest {

  private static final String NAMESPACE = "namespace";
  private static final String KEY = "key";

  @RegisterExtension
  private static final RedisClusterExtension CLUSTER_EXTENSION = RedisClusterExtension.builder().build();
  private TestClock clock;
  private PersistentTimer timer;

  @BeforeEach
  public void setup() {
    clock = TestClock.pinned(Instant.ofEpochSecond(10));
    timer = new PersistentTimer(CLUSTER_EXTENSION.getRedisCluster(), clock);
  }

  @Test
  public void testStop() {
    PersistentTimer.Sample sample = timer.start(NAMESPACE, KEY).join();
    final String redisKey = timer.redisKey(NAMESPACE, KEY);

    final String actualStartString = CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(conn -> conn.sync().get(redisKey));
    final Instant actualStart = Instant.ofEpochSecond(Long.parseLong(actualStartString));
    assertThat(actualStart).isEqualTo(clock.instant());

    final long ttl = CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(conn -> conn.sync().ttl(redisKey));

    assertThat(ttl).isBetween(0L, PersistentTimer.TIMER_TTL.getSeconds());

    Timer mockTimer = mock(Timer.class);
    clock.pin(clock.instant().plus(Duration.ofSeconds(5)));
    sample.stop(mockTimer).join();
    verify(mockTimer).record(Duration.ofSeconds(5));

    final String afterDeletion = CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(conn -> conn.sync().get(redisKey));

    assertThat(afterDeletion).isNull();
  }

  @Test
  public void testNamespace() {
    Timer mockTimer = mock(Timer.class);

    clock.pin(Instant.ofEpochSecond(10));
    PersistentTimer.Sample timer1 = timer.start("n1", KEY).join();
    clock.pin(Instant.ofEpochSecond(20));
    PersistentTimer.Sample timer2 = timer.start("n2", KEY).join();
    clock.pin(Instant.ofEpochSecond(30));

    timer2.stop(mockTimer).join();
    verify(mockTimer).record(Duration.ofSeconds(10));

    timer1.stop(mockTimer).join();
    verify(mockTimer).record(Duration.ofSeconds(20));
  }

  @Test
  public void testMultipleStart() {
    Timer mockTimer = mock(Timer.class);

    clock.pin(Instant.ofEpochSecond(10));
    PersistentTimer.Sample timer1 = timer.start(NAMESPACE, KEY).join();
    clock.pin(Instant.ofEpochSecond(11));
    PersistentTimer.Sample timer2 = timer.start(NAMESPACE, KEY).join();
    clock.pin(Instant.ofEpochSecond(12));
    PersistentTimer.Sample timer3 = timer.start(NAMESPACE, KEY).join();

    clock.pin(Instant.ofEpochSecond(20));
    timer2.stop(mockTimer).join();
    verify(mockTimer).record(Duration.ofSeconds(10));

    assertThatNoException().isThrownBy(() -> timer1.stop(mockTimer).join());
    assertThatNoException().isThrownBy(() -> timer3.stop(mockTimer).join());
  }


}
