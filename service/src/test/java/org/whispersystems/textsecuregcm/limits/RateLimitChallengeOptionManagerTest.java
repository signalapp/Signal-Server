/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.Account;

class RateLimitChallengeOptionManagerTest {

  private RateLimiters rateLimiters;

  private RateLimitChallengeOptionManager rateLimitChallengeOptionManager;

  @BeforeEach
  void setUp() {
    rateLimiters = mock(RateLimiters.class);
    rateLimitChallengeOptionManager = new RateLimitChallengeOptionManager(rateLimiters);
  }

  @ParameterizedTest
  @MethodSource
  void getChallengeOptions(final boolean captchaAttemptPermitted,
      final boolean captchaSuccessPermitted,
      final boolean pushAttemptPermitted,
      final boolean pushSuccessPermitted,
      final boolean expectCaptcha,
      final boolean expectPushChallenge) {

    final RateLimiter captchaChallengeAttemptLimiter = mock(RateLimiter.class);
    final RateLimiter captchaChallengeSuccessLimiter = mock(RateLimiter.class);
    final RateLimiter pushChallengeAttemptLimiter = mock(RateLimiter.class);
    final RateLimiter pushChallengeSuccessLimiter = mock(RateLimiter.class);

    when(rateLimiters.getCaptchaChallengeAttemptLimiter()).thenReturn(captchaChallengeAttemptLimiter);
    when(rateLimiters.getCaptchaChallengeSuccessLimiter()).thenReturn(captchaChallengeSuccessLimiter);
    when(rateLimiters.getPushChallengeAttemptLimiter()).thenReturn(pushChallengeAttemptLimiter);
    when(rateLimiters.getPushChallengeSuccessLimiter()).thenReturn(pushChallengeSuccessLimiter);

    when(captchaChallengeAttemptLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(
        captchaAttemptPermitted);
    when(captchaChallengeSuccessLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(
        captchaSuccessPermitted);
    when(pushChallengeAttemptLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(pushAttemptPermitted);
    when(pushChallengeSuccessLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(pushSuccessPermitted);

    final int expectedLength = (expectCaptcha ? 1 : 0) + (expectPushChallenge ? 1 : 0);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(UUID.randomUUID());

    final List<RateLimitChallengeOption> options = rateLimitChallengeOptionManager.getChallengeOptions(account);
    assertEquals(expectedLength, options.size());

    if (expectCaptcha) {
      assertTrue(options.contains(RateLimitChallengeOption.CAPTCHA));
    }

    if (expectPushChallenge) {
      assertTrue(options.contains(RateLimitChallengeOption.PUSH_CHALLENGE));
    }
  }

  private static Stream<Arguments> getChallengeOptions() {
    return Stream.of(
        Arguments.of(false, false, false, false, false, false),
        Arguments.of(false, false, false, true,  false, false),
        Arguments.of(false, false, true,  false, false, false),
        Arguments.of(false, false, true,  true,  false, true),
        Arguments.of(false, true,  false, false, false, false),
        Arguments.of(false, true,  false, true,  false, false),
        Arguments.of(false, true,  true,  false, false, false),
        Arguments.of(false, true,  true,  true,  false, true),
        Arguments.of(true,  false, false, false, false, false),
        Arguments.of(true,  false, false, true,  false, false),
        Arguments.of(true,  false, true,  false, false, false),
        Arguments.of(true,  false, true,  true,  false, true),
        Arguments.of(true,  true,  false, false, true,  false),
        Arguments.of(true,  true,  false, true,  true,  false),
        Arguments.of(true,  true,  true,  false, true,  false),
        Arguments.of(true,  true,  true,  true,  true,  true)
    );
  }
}
