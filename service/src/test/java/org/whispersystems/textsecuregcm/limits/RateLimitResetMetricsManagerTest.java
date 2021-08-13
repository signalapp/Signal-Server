package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.util.Duration;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;

class RateLimitResetMetricsManagerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private RateLimitResetMetricsManager metricsManager;
  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    metricsManager = new RateLimitResetMetricsManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(), meterRegistry);
  }

  @Test
  void testRecordMetrics() {

    final Account firstAccount = mock(Account.class);
    when(firstAccount.getUuid()).thenReturn(UUID.randomUUID());
    final Account secondAccount = mock(Account.class);
    when(secondAccount.getUuid()).thenReturn(UUID.randomUUID());

    metricsManager.recordMetrics(firstAccount, true, "counter", "enforced", "total", Duration.hours(1).toSeconds());
    metricsManager.recordMetrics(firstAccount, true, "counter", "enforced", "total", Duration.hours(1).toSeconds());
    metricsManager.recordMetrics(secondAccount, false, "counter", "unenforced", "total", Duration.hours(1).toSeconds());

    final double counterTotal = meterRegistry.get("counter").counters().stream()
        .map(Counter::count)
        .reduce(Double::sum)
        .orElseThrow();
    assertEquals(3, counterTotal, 0.0);

    final long enforcedCount = REDIS_CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(conn -> conn.sync().pfcount("enforced"));
    assertEquals(1L, enforcedCount);

    final long unenforcedCount = REDIS_CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(conn -> conn.sync().pfcount("unenforced"));
    assertEquals(1L, unenforcedCount);

    final long total = REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(conn -> conn.sync().pfcount("total"));
    assertEquals(2L, total);

  }
}
