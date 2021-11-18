/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.vdurmont.semver4j.Semver;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitChallengeConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

class RateLimitChallengeOptionManagerTest {

  private DynamicRateLimitChallengeConfiguration rateLimitChallengeConfiguration;
  private DynamicRateLimiters rateLimiters;

  private RateLimitChallengeOptionManager rateLimitChallengeOptionManager;

  @BeforeEach
  void setUp() {
    rateLimiters = mock(DynamicRateLimiters.class);

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    rateLimitChallengeConfiguration = mock(DynamicRateLimitChallengeConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getRateLimitChallengeConfiguration()).thenReturn(rateLimitChallengeConfiguration);

    rateLimitChallengeOptionManager = new RateLimitChallengeOptionManager(rateLimiters, dynamicConfigurationManager);
  }

  @ParameterizedTest
  @MethodSource
  void isClientBelowMinimumVersion(final String userAgent, final boolean expectBelowMinimumVersion) {
    when(rateLimitChallengeConfiguration.getMinimumSupportedVersion(any())).thenReturn(Optional.empty());
    when(rateLimitChallengeConfiguration.getMinimumSupportedVersion(ClientPlatform.ANDROID))
        .thenReturn(Optional.of(new Semver("5.6.0")));
    when(rateLimitChallengeConfiguration.getMinimumSupportedVersion(ClientPlatform.DESKTOP))
        .thenReturn(Optional.of(new Semver("5.0.0-beta.2")));

    assertEquals(expectBelowMinimumVersion, rateLimitChallengeOptionManager.isClientBelowMinimumVersion(userAgent));
  }

  private static Stream<Arguments> isClientBelowMinimumVersion() {
    return Stream.of(
        Arguments.of("Signal-Android/5.1.2 Android/30", true),
        Arguments.of("Signal-Android/5.6.0 Android/30", false),
        Arguments.of("Signal-Android/5.11.1 Android/30", false),
        Arguments.of("Signal-Desktop/5.0.0-beta.3 macOS/11", false),
        Arguments.of("Signal-Desktop/5.0.0-beta.1 Windows/3.1", true),
        Arguments.of("Signal-Desktop/5.2.0 Debian/11", false),
        Arguments.of("Signal-iOS/5.1.2 iOS/12.2", true),
        Arguments.of("anything-else", false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getChallengeOptions(final boolean captchaAttemptPermitted,
      final boolean captchaSuccessPermitted,
      final boolean pushAttemptPermitted,
      final boolean pushSuccessPermitted,
      final boolean expectCaptcha,
      final boolean expectPushChallenge) {

    final RateLimiter recaptchaChallengeAttemptLimiter = mock(RateLimiter.class);
    final RateLimiter recaptchaChallengeSuccessLimiter = mock(RateLimiter.class);
    final RateLimiter pushChallengeAttemptLimiter = mock(RateLimiter.class);
    final RateLimiter pushChallengeSuccessLimiter = mock(RateLimiter.class);

    when(rateLimiters.getRecaptchaChallengeAttemptLimiter()).thenReturn(recaptchaChallengeAttemptLimiter);
    when(rateLimiters.getRecaptchaChallengeSuccessLimiter()).thenReturn(recaptchaChallengeSuccessLimiter);
    when(rateLimiters.getPushChallengeAttemptLimiter()).thenReturn(pushChallengeAttemptLimiter);
    when(rateLimiters.getPushChallengeSuccessLimiter()).thenReturn(pushChallengeSuccessLimiter);

    when(recaptchaChallengeAttemptLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(captchaAttemptPermitted);
    when(recaptchaChallengeSuccessLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(captchaSuccessPermitted);
    when(pushChallengeAttemptLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(pushAttemptPermitted);
    when(pushChallengeSuccessLimiter.hasAvailablePermits(any(UUID.class), anyInt())).thenReturn(pushSuccessPermitted);

    final int expectedLength = (expectCaptcha ? 1 : 0) + (expectPushChallenge ? 1 : 0);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(UUID.randomUUID());

    final List<String> options = rateLimitChallengeOptionManager.getChallengeOptions(account);
    assertEquals(expectedLength, options.size());

    if (expectCaptcha) {
      assertTrue(options.contains(RateLimitChallengeOptionManager.OPTION_RECAPTCHA));
    }

    if (expectPushChallenge) {
      assertTrue(options.contains(RateLimitChallengeOptionManager.OPTION_PUSH_CHALLENGE));
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
