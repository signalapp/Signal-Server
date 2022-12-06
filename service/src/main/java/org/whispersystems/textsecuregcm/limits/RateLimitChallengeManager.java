package org.whispersystems.textsecuregcm.limits;

import static com.codahale.metrics.MetricRegistry.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.whispersystems.textsecuregcm.abuse.RateLimitChallengeListener;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Util;

public class RateLimitChallengeManager {

  private final PushChallengeManager pushChallengeManager;
  private final CaptchaChecker captchaChecker;
  private final DynamicRateLimiters rateLimiters;

  private final List<RateLimitChallengeListener> rateLimitChallengeListeners =
      Collections.synchronizedList(new ArrayList<>());

  private static final String RECAPTCHA_ATTEMPT_COUNTER_NAME = name(RateLimitChallengeManager.class, "recaptcha", "attempt");
  private static final String RESET_RATE_LIMIT_EXCEEDED_COUNTER_NAME = name(RateLimitChallengeManager.class, "resetRateLimitExceeded");

  private static final String SOURCE_COUNTRY_TAG_NAME = "sourceCountry";
  private static final String SUCCESS_TAG_NAME = "success";

  public RateLimitChallengeManager(
      final PushChallengeManager pushChallengeManager,
      final CaptchaChecker captchaChecker,
      final DynamicRateLimiters rateLimiters) {

    this.pushChallengeManager = pushChallengeManager;
    this.captchaChecker = captchaChecker;
    this.rateLimiters = rateLimiters;
  }

  public void addListener(final RateLimitChallengeListener rateLimitChallengeListener) {
    rateLimitChallengeListeners.add(rateLimitChallengeListener);
  }

  public void answerPushChallenge(final Account account, final String challenge) throws RateLimitExceededException {
    rateLimiters.getPushChallengeAttemptLimiter().validate(account.getUuid());

    final boolean challengeSuccess = pushChallengeManager.answerChallenge(account, challenge);

    if (challengeSuccess) {
      rateLimiters.getPushChallengeSuccessLimiter().validate(account.getUuid());
      resetRateLimits(account);
    }
  }

  public void answerRecaptchaChallenge(final Account account, final String captcha, final String mostRecentProxyIp, final String userAgent)
      throws RateLimitExceededException, IOException {

    rateLimiters.getRecaptchaChallengeAttemptLimiter().validate(account.getUuid());

    final boolean challengeSuccess = captchaChecker.verify(captcha, mostRecentProxyIp).valid();

    final Tags tags = Tags.of(
        Tag.of(SOURCE_COUNTRY_TAG_NAME, Util.getCountryCode(account.getNumber())),
        Tag.of(SUCCESS_TAG_NAME, String.valueOf(challengeSuccess)),
        UserAgentTagUtil.getPlatformTag(userAgent)
    );

    Metrics.counter(RECAPTCHA_ATTEMPT_COUNTER_NAME, tags).increment();

    if (challengeSuccess) {
      rateLimiters.getRecaptchaChallengeSuccessLimiter().validate(account.getUuid());
      resetRateLimits(account);
    }
  }

  private void resetRateLimits(final Account account) throws RateLimitExceededException {
    try {
      rateLimiters.getRateLimitResetLimiter().validate(account.getUuid());
    } catch (final RateLimitExceededException e) {
      Metrics.counter(RESET_RATE_LIMIT_EXCEEDED_COUNTER_NAME,
          SOURCE_COUNTRY_TAG_NAME, Util.getCountryCode(account.getNumber())).increment();

      throw e;
    }

    rateLimitChallengeListeners.forEach(listener -> listener.handleRateLimitChallengeAnswered(account));
  }

  public void sendPushChallenge(final Account account) throws NotPushRegisteredException {
    pushChallengeManager.sendChallenge(account);
  }
}
