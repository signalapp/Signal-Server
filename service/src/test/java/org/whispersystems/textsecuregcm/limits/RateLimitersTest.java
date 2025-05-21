/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;

@SuppressWarnings("unchecked")
public class RateLimitersTest {

  private final DynamicConfiguration configuration = mock(DynamicConfiguration.class);

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfig =
      MockUtils.buildMock(DynamicConfigurationManager.class, cfg -> when(cfg.getConfiguration()).thenReturn(configuration));

  private final ClusterLuaScript validateScript = mock(ClusterLuaScript.class);

  private final FaultTolerantRedisClusterClient redisCluster = mock(FaultTolerantRedisClusterClient.class);

  private final MutableClock clock = MockUtils.mutableClock(0);

  @Test
  public void testValidateDuplicates() throws Exception {
    final TestDescriptor td1 = new TestDescriptor("id1");
    final TestDescriptor td2 = new TestDescriptor("id2");
    final TestDescriptor td3 = new TestDescriptor("id3");
    final TestDescriptor tdDup = new TestDescriptor("id1");

    assertThrows(IllegalStateException.class, () -> new BaseRateLimiters<>(
        new TestDescriptor[] { td1, td2, td3, tdDup },
        dynamicConfig,
        validateScript,
        redisCluster,
        clock) {});

    new BaseRateLimiters<>(
        new TestDescriptor[] { td1, td2, td3 },
        dynamicConfig,
        validateScript,
        redisCluster,
        clock) {};
  }

  @Test
  void testUnchangingConfiguration() {
    final RateLimiters rateLimiters = new RateLimiters(dynamicConfig, validateScript, redisCluster, clock);
    final RateLimiter limiter = rateLimiters.getRateLimitResetLimiter();
    final RateLimiterConfig expected = RateLimiters.For.RATE_LIMIT_RESET.defaultConfig();
    assertEquals(expected, limiter.config());
  }

  @Test
  void testChangingConfiguration() {
    final RateLimiterConfig initialRateLimiterConfig = new RateLimiterConfig(4, Duration.ofMinutes(1), false);
    final RateLimiterConfig updatedRateLimiterCongig = new RateLimiterConfig(17, Duration.ofSeconds(3), false);
    final RateLimiterConfig baseConfig = new RateLimiterConfig(1, Duration.ofMinutes(1), false);

    final Map<String, RateLimiterConfig> limitsConfigMap = new HashMap<>();

    limitsConfigMap.put(RateLimiters.For.CAPTCHA_CHALLENGE_ATTEMPT.id(), baseConfig);
    limitsConfigMap.put(RateLimiters.For.CAPTCHA_CHALLENGE_SUCCESS.id(), baseConfig);

    when(configuration.getLimits()).thenReturn(limitsConfigMap);

    final RateLimiters rateLimiters = new RateLimiters(dynamicConfig, validateScript, redisCluster, clock);
    final RateLimiter limiter = rateLimiters.getRateLimitResetLimiter();

    limitsConfigMap.put(RateLimiters.For.RATE_LIMIT_RESET.id(), initialRateLimiterConfig);
    assertEquals(initialRateLimiterConfig, limiter.config());

    assertEquals(baseConfig, rateLimiters.getCaptchaChallengeAttemptLimiter().config());
    assertEquals(baseConfig, rateLimiters.getCaptchaChallengeSuccessLimiter().config());

    limitsConfigMap.put(RateLimiters.For.RATE_LIMIT_RESET.id(), updatedRateLimiterCongig);
    assertEquals(updatedRateLimiterCongig, limiter.config());

    assertEquals(baseConfig, rateLimiters.getCaptchaChallengeAttemptLimiter().config());
    assertEquals(baseConfig, rateLimiters.getCaptchaChallengeSuccessLimiter().config());
  }

  @Test
  public void testRateLimiterHasItsPrioritiesStraight() throws Exception {
    final RateLimiters.For descriptor = RateLimiters.For.CAPTCHA_CHALLENGE_ATTEMPT;
    final RateLimiterConfig configForDynamic = new RateLimiterConfig(1, Duration.ofMinutes(1), false);
    final RateLimiterConfig defaultConfig = descriptor.defaultConfig();

    final Map<String, RateLimiterConfig> mapForDynamic = new HashMap<>();

    when(configuration.getLimits()).thenReturn(mapForDynamic);

    final RateLimiters rateLimiters = new RateLimiters(dynamicConfig, validateScript, redisCluster, clock);
    final RateLimiter limiter = rateLimiters.forDescriptor(descriptor);

    // test only default is present
    mapForDynamic.remove(descriptor.id());
    assertEquals(defaultConfig, limiter.config());

    // test dynamic config is present
    mapForDynamic.put(descriptor.id(), configForDynamic);
    assertEquals(configForDynamic, limiter.config());
  }

  private record TestDescriptor(String id) implements RateLimiterDescriptor {

    @Override
    public RateLimiterConfig defaultConfig() {
      return new RateLimiterConfig(1, Duration.ofMinutes(1), false);
    }
  }
}
