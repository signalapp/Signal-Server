package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.vdurmont.semver4j.Semver;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRateLimitChallengeConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

class RateLimitChallengeManagerTest {

  private PushChallengeManager pushChallengeManager;
  private RecaptchaClient recaptchaClient;
  private PreKeyRateLimiter preKeyRateLimiter;
  private UnsealedSenderRateLimiter unsealedSenderRateLimiter;
  private DynamicRateLimitChallengeConfiguration rateLimitChallengeConfiguration;
  private RateLimiters rateLimiters;

  private RateLimitChallengeManager rateLimitChallengeManager;

  @BeforeEach
  void setUp() {
    pushChallengeManager = mock(PushChallengeManager.class);
    recaptchaClient = mock(RecaptchaClient.class);
    preKeyRateLimiter = mock(PreKeyRateLimiter.class);
    unsealedSenderRateLimiter = mock(UnsealedSenderRateLimiter.class);
    rateLimiters = mock(RateLimiters.class);

    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    rateLimitChallengeConfiguration = mock(DynamicRateLimitChallengeConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getRateLimitChallengeConfiguration()).thenReturn(rateLimitChallengeConfiguration);

    rateLimitChallengeManager = new RateLimitChallengeManager(
        pushChallengeManager,
        recaptchaClient,
        preKeyRateLimiter,
        unsealedSenderRateLimiter,
        rateLimiters,
        dynamicConfigurationManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void answerPushChallenge(final boolean successfulChallenge) throws RateLimitExceededException {
    final Account account = mock(Account.class);
    when(pushChallengeManager.answerChallenge(eq(account), any())).thenReturn(successfulChallenge);

    when(rateLimiters.getPushChallengeAttemptLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getPushChallengeSuccessLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getRateLimitResetLimiter()).thenReturn(mock(RateLimiter.class));

    rateLimitChallengeManager.answerPushChallenge(account, "challenge");

    if (successfulChallenge) {
      verify(preKeyRateLimiter).handleRateLimitReset(account);
      verify(unsealedSenderRateLimiter).handleRateLimitReset(account);
    } else {
      verifyZeroInteractions(preKeyRateLimiter);
      verifyZeroInteractions(unsealedSenderRateLimiter);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void answerRecaptchaChallenge(final boolean successfulChallenge) throws RateLimitExceededException {
    final Account account = mock(Account.class);
    when(recaptchaClient.verify(any(), any())).thenReturn(successfulChallenge);

    when(rateLimiters.getRecaptchaChallengeAttemptLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getRecaptchaChallengeSuccessLimiter()).thenReturn(mock(RateLimiter.class));
    when(rateLimiters.getRateLimitResetLimiter()).thenReturn(mock(RateLimiter.class));

    rateLimitChallengeManager.answerRecaptchaChallenge(account, "captcha", "10.0.0.1");

    if (successfulChallenge) {
      verify(preKeyRateLimiter).handleRateLimitReset(account);
      verify(unsealedSenderRateLimiter).handleRateLimitReset(account);
    } else {
      verifyZeroInteractions(preKeyRateLimiter);
      verifyZeroInteractions(unsealedSenderRateLimiter);
    }
  }

  @ParameterizedTest
  @MethodSource
  void shouldIssueRateLimitChallenge(final String userAgent, final boolean expectIssueChallenge) {
    when(rateLimitChallengeConfiguration.getMinimumSupportedVersion(any())).thenReturn(Optional.empty());
    when(rateLimitChallengeConfiguration.getMinimumSupportedVersion(ClientPlatform.ANDROID))
        .thenReturn(Optional.of(new Semver("5.6.0")));
    when(rateLimitChallengeConfiguration.getMinimumSupportedVersion(ClientPlatform.DESKTOP))
        .thenReturn(Optional.of(new Semver("5.0.0-beta.2")));

    assertEquals(expectIssueChallenge, rateLimitChallengeManager.shouldIssueRateLimitChallenge(userAgent));
  }

  private static Stream<Arguments> shouldIssueRateLimitChallenge() {
    return Stream.of(
        Arguments.of("Signal-Android/5.1.2 Android/30", false),
        Arguments.of("Signal-Android/5.6.0 Android/30", true),
        Arguments.of("Signal-Android/5.11.1 Android/30", true),
        Arguments.of("Signal-Desktop/5.0.0-beta.3 macOS/11", true),
        Arguments.of("Signal-Desktop/5.0.0-beta.1 Windows/3.1", false),
        Arguments.of("Signal-Desktop/5.2.0 Debian/11", true),
        Arguments.of("Signal-iOS/5.1.2 iOS/12.2", false),
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

    when(recaptchaChallengeAttemptLimiter.hasAvailablePermits(any(), anyInt())).thenReturn(captchaAttemptPermitted);
    when(recaptchaChallengeSuccessLimiter.hasAvailablePermits(any(), anyInt())).thenReturn(captchaSuccessPermitted);
    when(pushChallengeAttemptLimiter.hasAvailablePermits(any(), anyInt())).thenReturn(pushAttemptPermitted);
    when(pushChallengeSuccessLimiter.hasAvailablePermits(any(), anyInt())).thenReturn(pushSuccessPermitted);

    final int expectedLength = (expectCaptcha ? 1 : 0) + (expectPushChallenge ? 1 : 0);

    final List<String> options = rateLimitChallengeManager.getChallengeOptions(mock(Account.class));
    assertEquals(expectedLength, options.size());

    if (expectCaptcha) {
      assertTrue(options.contains(RateLimitChallengeManager.OPTION_RECAPTCHA));
    }

    if (expectPushChallenge) {
      assertTrue(options.contains(RateLimitChallengeManager.OPTION_PUSH_CHALLENGE));
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
