/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.captcha.Action;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.spam.ChallengeType;
import org.whispersystems.textsecuregcm.spam.RateLimitChallengeListener;
import org.whispersystems.textsecuregcm.storage.Account;

class RateLimitChallengeManagerTest {

  private static final float DEFAULT_SCORE_THRESHOLD = 0.1f;

  private PushChallengeManager pushChallengeManager;
  private CaptchaChecker captchaChecker;
  private RateLimiters rateLimiters;
  private RateLimitChallengeListener rateLimitChallengeListener;

  private RateLimitChallengeManager rateLimitChallengeManager;

  @BeforeEach
  void setUp() {
    pushChallengeManager = mock(PushChallengeManager.class);
    captchaChecker = mock(CaptchaChecker.class);
    rateLimiters = mock(RateLimiters.class);
    rateLimitChallengeListener = mock(RateLimitChallengeListener.class);

    rateLimitChallengeManager = new RateLimitChallengeManager(
        pushChallengeManager,
        captchaChecker,
        rateLimiters,
        List.of(rateLimitChallengeListener));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void answerPushChallenge(final boolean successfulChallenge) throws RateLimitExceededException {
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(UUID.randomUUID());

    when(pushChallengeManager.answerChallenge(eq(account), any())).thenReturn(successfulChallenge);

    when(rateLimiters.getPushChallengeAttemptLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getPushChallengeSuccessLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getRateLimitResetLimiter()).thenReturn(mock(RateLimiter.class));

    rateLimitChallengeManager.answerPushChallenge(account, "challenge");

    if (successfulChallenge) {
      verify(rateLimitChallengeListener).handleRateLimitChallengeAnswered(account, ChallengeType.PUSH);
    } else {
      verifyNoInteractions(rateLimitChallengeListener);
    }
  }

  @ParameterizedTest
  @MethodSource
  void answerCaptchaChallenge(Optional<Float> scoreThreshold, float actualScore, boolean expectSuccess)
    throws RateLimitExceededException, IOException {
    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn("+18005551234");
    when(account.getUuid()).thenReturn(UUID.randomUUID());

    when(captchaChecker.verify(any(), eq(Action.CHALLENGE), any(), any(), any()))
        .thenReturn(AssessmentResult.fromScore(actualScore, DEFAULT_SCORE_THRESHOLD));

    when(rateLimiters.getCaptchaChallengeAttemptLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getCaptchaChallengeSuccessLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getRateLimitResetLimiter()).thenReturn(mock(RateLimiter.class));

    rateLimitChallengeManager.answerCaptchaChallenge(account, "captcha", "10.0.0.1", "Test User-Agent", scoreThreshold);

    if (expectSuccess) {
      verify(rateLimitChallengeListener).handleRateLimitChallengeAnswered(account, ChallengeType.CAPTCHA);
    } else {
      verifyNoInteractions(rateLimitChallengeListener);
    }
  }

  private static Stream<Arguments> answerCaptchaChallenge() {
    return Stream.of(
        Arguments.of(Optional.empty(), 0.5f, true),
        Arguments.of(Optional.empty(), 0.1f, true),
        Arguments.of(Optional.empty(), 0.0f, false),
        Arguments.of(Optional.of(0.1f), 0.5f, true),
        Arguments.of(Optional.of(0.1f), 0.1f, true),
        Arguments.of(Optional.of(0.1f), 0.0f, false),
        Arguments.of(Optional.of(0.3f), 0.5f, true),
        Arguments.of(Optional.of(0.3f), 0.1f, false),
        Arguments.of(Optional.of(0.3f), 0.0f, false));
  }
}
