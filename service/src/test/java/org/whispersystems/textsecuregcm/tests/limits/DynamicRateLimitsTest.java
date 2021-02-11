package org.whispersystems.textsecuregcm.tests.limits;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitsConfiguration;
import org.whispersystems.textsecuregcm.limits.CardinalityRateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicRateLimitsTest {

  private DynamicConfigurationManager dynamicConfig;
  private FaultTolerantRedisCluster   redisCluster;

  @Before
  public void setup() {
    this.dynamicConfig = mock(DynamicConfigurationManager.class);
    this.redisCluster  = mock(FaultTolerantRedisCluster.class);

    DynamicConfiguration defaultConfig = new DynamicConfiguration();
    when(dynamicConfig.getConfiguration()).thenReturn(defaultConfig);

  }

  @Test
  public void testUnchangingConfiguration() {
    RateLimiters rateLimiters = new RateLimiters(new RateLimitsConfiguration(), dynamicConfig, redisCluster);

    RateLimiter limiter = rateLimiters.getUnsealedIpLimiter();

    assertThat(limiter.getBucketSize()).isEqualTo(dynamicConfig.getConfiguration().getLimits().getUnsealedSenderIp().getBucketSize());
    assertThat(limiter.getLeakRatePerMinute()).isEqualTo(dynamicConfig.getConfiguration().getLimits().getUnsealedSenderIp().getLeakRatePerMinute());
    assertSame(rateLimiters.getUnsealedIpLimiter(), limiter);
  }

  @Test
  public void testChangingConfiguration() {
    DynamicConfiguration configuration = mock(DynamicConfiguration.class);
    DynamicRateLimitsConfiguration limitsConfiguration = mock(DynamicRateLimitsConfiguration.class);

    when(configuration.getLimits()).thenReturn(limitsConfiguration);
    when(limitsConfiguration.getUnsealedSenderNumber()).thenReturn(new RateLimitsConfiguration.CardinalityRateLimitConfiguration(10, Duration.ofHours(1), Duration.ofMinutes(10)));
    when(limitsConfiguration.getUnsealedSenderIp()).thenReturn(new RateLimitsConfiguration.RateLimitConfiguration(4, 1.0));

    when(dynamicConfig.getConfiguration()).thenReturn(configuration);

    RateLimiters rateLimiters = new RateLimiters(new RateLimitsConfiguration(), dynamicConfig, redisCluster);

    CardinalityRateLimiter limiter = rateLimiters.getUnsealedSenderLimiter();

    assertThat(limiter.getMaxCardinality()).isEqualTo(10);
    assertThat(limiter.getTtl()).isEqualTo(Duration.ofHours(1));
    assertThat(limiter.getTtlJitter()).isEqualTo(Duration.ofMinutes(10));
    assertSame(rateLimiters.getUnsealedSenderLimiter(), limiter);

    when(limitsConfiguration.getUnsealedSenderNumber()).thenReturn(new RateLimitsConfiguration.CardinalityRateLimitConfiguration(20, Duration.ofHours(2), Duration.ofMinutes(7)));

    CardinalityRateLimiter changed = rateLimiters.getUnsealedSenderLimiter();

    assertThat(changed.getMaxCardinality()).isEqualTo(20);
    assertThat(changed.getTtl()).isEqualTo(Duration.ofHours(2));
    assertThat(changed.getTtlJitter()).isEqualTo(Duration.ofMinutes(7));
    assertNotSame(limiter, changed);

  }

}
