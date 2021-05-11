/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMessageRateConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitChallengeConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitsConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class UnsealedSenderRateLimiterTest extends AbstractRedisClusterTest {

  private Account sender;
  private Account firstDestination;
  private Account secondDestination;

  private UnsealedSenderRateLimiter unsealedSenderRateLimiter;

  private DynamicRateLimitChallengeConfiguration rateLimitChallengeConfiguration;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    final CardinalityRateLimiter cardinalityRateLimiter =
        new CardinalityRateLimiter(getRedisCluster(), "test", Duration.ofDays(1), 1);

    when(rateLimiters.getUnsealedSenderCardinalityLimiter()).thenReturn(cardinalityRateLimiter);
    when(rateLimiters.getRateLimitResetLimiter()).thenReturn(mock(RateLimiter.class));

    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicRateLimitsConfiguration rateLimitsConfiguration = mock(DynamicRateLimitsConfiguration.class);
    rateLimitChallengeConfiguration = mock(DynamicRateLimitChallengeConfiguration.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getLimits()).thenReturn(rateLimitsConfiguration);
    when(rateLimitsConfiguration.getUnsealedSenderDefaultCardinalityLimit()).thenReturn(1);
    when(rateLimitsConfiguration.getUnsealedSenderPermitIncrement()).thenReturn(1);
    when(dynamicConfiguration.getRateLimitChallengeConfiguration()).thenReturn(rateLimitChallengeConfiguration);
    when(rateLimitChallengeConfiguration.isUnsealedSenderLimitEnforced()).thenReturn(true);

    unsealedSenderRateLimiter = new UnsealedSenderRateLimiter(rateLimiters, getRedisCluster(), dynamicConfigurationManager,
        mock(RateLimitResetMetricsManager.class));

    sender = mock(Account.class);
    when(sender.getNumber()).thenReturn("+18005551111");
    when(sender.getUuid()).thenReturn(UUID.randomUUID());

    firstDestination = mock(Account.class);
    when(firstDestination.getNumber()).thenReturn("+18005552222");
    when(firstDestination.getUuid()).thenReturn(UUID.randomUUID());

    secondDestination = mock(Account.class);
    when(secondDestination.getNumber()).thenReturn("+18005553333");
    when(secondDestination.getUuid()).thenReturn(UUID.randomUUID());
  }

  @Test
  public void validate() throws RateLimitExceededException {
    unsealedSenderRateLimiter.validate(sender, firstDestination);

    assertThrows(RateLimitExceededException.class, () -> unsealedSenderRateLimiter.validate(sender, secondDestination));

    unsealedSenderRateLimiter.validate(sender, firstDestination);
  }

  @Test
  public void handleRateLimitReset() throws RateLimitExceededException {
    unsealedSenderRateLimiter.validate(sender, firstDestination);

    assertThrows(RateLimitExceededException.class, () -> unsealedSenderRateLimiter.validate(sender, secondDestination));

    unsealedSenderRateLimiter.handleRateLimitReset(sender);
    unsealedSenderRateLimiter.validate(sender, firstDestination);
    unsealedSenderRateLimiter.validate(sender, secondDestination);
  }

  @Test
  public void enforcementConfiguration() throws RateLimitExceededException {

    when(rateLimitChallengeConfiguration.isUnsealedSenderLimitEnforced()).thenReturn(false);

    unsealedSenderRateLimiter.validate(sender, firstDestination);
    unsealedSenderRateLimiter.validate(sender, secondDestination);

    when(rateLimitChallengeConfiguration.isUnsealedSenderLimitEnforced()).thenReturn(true);

    final Account thirdDestination = mock(Account.class);
    when(thirdDestination.getNumber()).thenReturn("+18005554444");
    when(thirdDestination.getUuid()).thenReturn(UUID.randomUUID());

    assertThrows(RateLimitExceededException.class, () -> unsealedSenderRateLimiter.validate(sender, thirdDestination));

    when(rateLimitChallengeConfiguration.isUnsealedSenderLimitEnforced()).thenReturn(false);

    final Account fourthDestination = mock(Account.class);
    when(fourthDestination.getNumber()).thenReturn("+18005555555");
    when(fourthDestination.getUuid()).thenReturn(UUID.randomUUID());
    unsealedSenderRateLimiter.validate(sender, fourthDestination);
  }
}
