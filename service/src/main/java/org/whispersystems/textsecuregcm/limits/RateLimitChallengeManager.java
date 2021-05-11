package org.whispersystems.textsecuregcm.limits;

import com.vdurmont.semver4j.Semver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

public class RateLimitChallengeManager {

  private final PushChallengeManager pushChallengeManager;
  private final RecaptchaClient recaptchaClient;

  private final PreKeyRateLimiter preKeyRateLimiter;
  private final UnsealedSenderRateLimiter unsealedSenderRateLimiter;

  private final RateLimiters rateLimiters;
  private final DynamicConfigurationManager dynamicConfigurationManager;

  public static final String OPTION_RECAPTCHA = "recaptcha";
  public static final String OPTION_PUSH_CHALLENGE = "pushChallenge";

  public RateLimitChallengeManager(
      final PushChallengeManager pushChallengeManager,
      final RecaptchaClient recaptchaClient,
      final PreKeyRateLimiter preKeyRateLimiter,
      final UnsealedSenderRateLimiter unsealedSenderRateLimiter,
      final RateLimiters rateLimiters,
      final DynamicConfigurationManager dynamicConfigurationManager) {

    this.pushChallengeManager = pushChallengeManager;
    this.recaptchaClient = recaptchaClient;
    this.preKeyRateLimiter = preKeyRateLimiter;
    this.unsealedSenderRateLimiter = unsealedSenderRateLimiter;
    this.rateLimiters = rateLimiters;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public void answerPushChallenge(final Account account, final String challenge) throws RateLimitExceededException {
    rateLimiters.getPushChallengeAttemptLimiter().validate(account.getNumber());

    final boolean challengeSuccess = pushChallengeManager.answerChallenge(account, challenge);

    if (challengeSuccess) {
      rateLimiters.getPushChallengeSuccessLimiter().validate(account.getNumber());
      resetRateLimits(account);
    }
  }

  public void answerRecaptchaChallenge(final Account account, final String captcha, final String mostRecentProxyIp)
      throws RateLimitExceededException {

    rateLimiters.getRecaptchaChallengeAttemptLimiter().validate(account.getNumber());

    final boolean challengeSuccess = recaptchaClient.verify(captcha, mostRecentProxyIp);

    if (challengeSuccess) {
      rateLimiters.getRecaptchaChallengeSuccessLimiter().validate(account.getNumber());
      resetRateLimits(account);
    }
  }

  private void resetRateLimits(final Account account) throws RateLimitExceededException {
    rateLimiters.getRateLimitResetLimiter().validate(account.getNumber());

    preKeyRateLimiter.handleRateLimitReset(account);
    unsealedSenderRateLimiter.handleRateLimitReset(account);
  }

  public boolean shouldIssueRateLimitChallenge(final String userAgent) {
    try {
      final UserAgent client = UserAgentUtil.parseUserAgentString(userAgent);
      final Optional<Semver> minimumClientVersion = dynamicConfigurationManager.getConfiguration()
          .getRateLimitChallengeConfiguration()
          .getMinimumSupportedVersion(client.getPlatform());

      return minimumClientVersion.map(version -> version.isLowerThanOrEqualTo(client.getVersion()))
          .orElse(false);
    } catch (final UnrecognizedUserAgentException ignored) {
      return false;
    }
  }

  public List<String> getChallengeOptions(final Account account) {
    final List<String> options = new ArrayList<>(2);

    final String key = account.getNumber();

    if (rateLimiters.getRecaptchaChallengeAttemptLimiter().hasAvailablePermits(key, 1) &&
        rateLimiters.getRecaptchaChallengeSuccessLimiter().hasAvailablePermits(key, 1)) {

      options.add(OPTION_RECAPTCHA);
    }

    if (rateLimiters.getPushChallengeAttemptLimiter().hasAvailablePermits(key, 1) &&
        rateLimiters.getPushChallengeSuccessLimiter().hasAvailablePermits(key, 1)) {

      options.add(OPTION_PUSH_CHALLENGE);
    }

    return options;
  }

  public void sendPushChallenge(final Account account) throws NotPushRegisteredException {
    pushChallengeManager.sendChallenge(account);
  }
}
