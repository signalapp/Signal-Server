package org.whispersystems.textsecuregcm.tests.limits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.RateLimitsConfiguration.RateLimitConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitsConfiguration;
import org.whispersystems.textsecuregcm.limits.DynamicRateLimiters;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class DynamicRateLimitsTest {

  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfig;
  private FaultTolerantRedisCluster redisCluster;

  @BeforeEach
  void setup() {
    this.dynamicConfig = mock(DynamicConfigurationManager.class);
    this.redisCluster  = mock(FaultTolerantRedisCluster.class);

    DynamicConfiguration defaultConfig = new DynamicConfiguration();
    when(dynamicConfig.getConfiguration()).thenReturn(defaultConfig);

  }

  @Test
  void testUnchangingConfiguration() {
    DynamicRateLimiters rateLimiters = new DynamicRateLimiters(redisCluster, dynamicConfig);

    RateLimiter limiter = rateLimiters.getRateLimitResetLimiter();

    assertThat(limiter.getBucketSize()).isEqualTo(dynamicConfig.getConfiguration().getLimits().getRateLimitReset().getBucketSize());
    assertThat(limiter.getLeakRatePerMinute()).isEqualTo(dynamicConfig.getConfiguration().getLimits().getRateLimitReset().getLeakRatePerMinute());
    assertSame(rateLimiters.getRateLimitResetLimiter(), limiter);
  }

  @Test
  void testChangingConfiguration() {
    DynamicConfiguration configuration = mock(DynamicConfiguration.class);
    DynamicRateLimitsConfiguration limitsConfiguration = mock(DynamicRateLimitsConfiguration.class);

    when(configuration.getLimits()).thenReturn(limitsConfiguration);
    when(limitsConfiguration.getRecaptchaChallengeAttempt()).thenReturn(new RateLimitConfiguration());
    when(limitsConfiguration.getRecaptchaChallengeSuccess()).thenReturn(new RateLimitConfiguration());
    when(limitsConfiguration.getPushChallengeAttempt()).thenReturn(new RateLimitConfiguration());
    when(limitsConfiguration.getPushChallengeSuccess()).thenReturn(new RateLimitConfiguration());

    final RateLimitConfiguration initialRateLimitConfiguration = new RateLimitConfiguration(4, 1);
    when(limitsConfiguration.getRateLimitReset()).thenReturn(initialRateLimitConfiguration);

    when(dynamicConfig.getConfiguration()).thenReturn(configuration);

    DynamicRateLimiters rateLimiters = new DynamicRateLimiters(redisCluster, dynamicConfig);

    RateLimiter limiter = rateLimiters.getRateLimitResetLimiter();

    assertThat(limiter.getBucketSize()).isEqualTo(4);
    assertThat(limiter.getLeakRatePerMinute()).isEqualTo(1);
    assertSame(rateLimiters.getRateLimitResetLimiter(), limiter);

    when(limitsConfiguration.getRateLimitReset()).thenReturn(new RateLimitConfiguration(17, 19));

    RateLimiter changed = rateLimiters.getRateLimitResetLimiter();

    assertThat(changed.getBucketSize()).isEqualTo(17);
    assertThat(changed.getLeakRatePerMinute()).isEqualTo(19);
    assertNotSame(limiter, changed);
  }

}
