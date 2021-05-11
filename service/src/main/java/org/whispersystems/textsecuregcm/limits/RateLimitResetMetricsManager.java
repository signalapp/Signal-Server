package org.whispersystems.textsecuregcm.limits;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.Account;

public class RateLimitResetMetricsManager {

  private final FaultTolerantRedisCluster metricsCluster;
  private final MeterRegistry meterRegistry;

  public RateLimitResetMetricsManager(
      final FaultTolerantRedisCluster metricsCluster, final MeterRegistry meterRegistry) {
    this.metricsCluster = metricsCluster;
    this.meterRegistry = meterRegistry;
  }

  void initializeFunctionCounters(String counterKey, String hllKey) {
    FunctionCounter.builder(counterKey, null, (ignored) ->
        metricsCluster.<Long>withCluster(conn -> conn.sync().pfcount(hllKey))).register(meterRegistry);
  }

  void recordMetrics(Account account, boolean enforced, String counterKey, String hllEnforcedKey, String hllTotalKey,
      long hllTtl) {

    Counter.builder(counterKey)
        .tag("enforced", String.valueOf(enforced))
        .register(meterRegistry)
        .increment();

    metricsCluster.useCluster(connection -> {
      connection.sync().pfadd(hllEnforcedKey, account.getUuid().toString());
      connection.sync().expire(hllEnforcedKey, hllTtl);
      connection.sync().pfadd(hllTotalKey, account.getUuid().toString());
      connection.sync().expire(hllTotalKey, hllTtl);
    });
  }
}
