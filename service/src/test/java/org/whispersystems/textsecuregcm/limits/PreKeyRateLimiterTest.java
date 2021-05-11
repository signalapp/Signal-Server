package org.whispersystems.textsecuregcm.limits;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitChallengeConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class PreKeyRateLimiterTest {

  private Account account;

  private PreKeyRateLimiter preKeyRateLimiter;

  private DynamicRateLimitChallengeConfiguration rateLimitChallengeConfiguration;
  private RateLimiter dailyPreKeyLimiter;

  @BeforeEach
  void setup() {
    final RateLimiters rateLimiters = mock(RateLimiters.class);

    dailyPreKeyLimiter = mock(RateLimiter.class);
    when(rateLimiters.getDailyPreKeysLimiter()).thenReturn(dailyPreKeyLimiter);

    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    rateLimitChallengeConfiguration = mock(DynamicRateLimitChallengeConfiguration.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getRateLimitChallengeConfiguration()).thenReturn(rateLimitChallengeConfiguration);

    preKeyRateLimiter = new PreKeyRateLimiter(rateLimiters, dynamicConfigurationManager, mock(RateLimitResetMetricsManager.class));

    account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551111");
    when(account.getUuid()).thenReturn(UUID.randomUUID());
  }

  @Test
  void enforcementConfiguration() throws RateLimitExceededException {

    doThrow(RateLimitExceededException.class)
      .when(dailyPreKeyLimiter).validate(any());

    when(rateLimitChallengeConfiguration.isPreKeyLimitEnforced()).thenReturn(false);

    preKeyRateLimiter.validate(account);

    when(rateLimitChallengeConfiguration.isPreKeyLimitEnforced()).thenReturn(true);

    assertThrows(RateLimitExceededException.class, () -> preKeyRateLimiter.validate(account));

    when(rateLimitChallengeConfiguration.isPreKeyLimitEnforced()).thenReturn(false);

    preKeyRateLimiter.validate(account);
  }
}
