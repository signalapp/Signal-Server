/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.lifecycle.Managed;
import jakarta.validation.Validator;
import java.io.IOException;
import java.util.function.Function;
import org.whispersystems.textsecuregcm.captcha.CaptchaClient;
import org.whispersystems.textsecuregcm.storage.ReportedMessageListener;

/**
 * A spam filter provides various checkers and listeners to detect and respond to patterns of spam and fraud.
 * <p/>
 * Spam filters are managed components that are generally loaded dynamically via a {@link java.util.ServiceLoader}.
 * Their {@link #configure(String, Validator)} method will be called prior to be adding to the server's pool of {@link Managed}
 * objects.
 * <p/>
 */
public interface SpamFilter extends Managed {

  /**
   * Configures this spam filter. This method will be called before the filter is added to the server's pool of managed
   * objects and before the server processes any requests.
   *
   * @param environmentName the name of the environment in which this filter is running (e.g. "staging" or
   *                        "production")
   * @param validator may be used to validate configuration
   * @throws IOException if the filter could not read its configuration source for any reason
   * @throws ConfigurationValidationException if the configuration failed validation
   */
  void configure(String environmentName, Validator validator) throws IOException, ConfigurationValidationException;

  /**
   * Return a reported message listener controlled by the spam filter. Listeners will be registered with the
   * {@link org.whispersystems.textsecuregcm.storage.ReportMessageManager}.
   *
   * @return a reported message listener controlled by the spam filter
   */
  ReportedMessageListener getReportedMessageListener();

  /**
   * Return a rate limit challenge listener. Listeners will be registered with the
   * {@link org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager}
   *
   * @return a {@link RateLimitChallengeListener} controlled by the spam filter
   */
  RateLimitChallengeListener getRateLimitChallengeListener();

  /**
   * Return a spam checker that will be called on message sends via the
   * {@link org.whispersystems.textsecuregcm.controllers.MessageController} to determine whether a specific message
   * spend is spam.
   *
   * @return a {@link SpamChecker} controlled by the spam filter
   */
  SpamChecker getSpamChecker();

  /**
   * Return a checker that will be called to check registration attempts
   *
   * @return a {@link RegistrationFraudChecker} controlled by the spam filter
   */
  RegistrationFraudChecker getRegistrationFraudChecker();

  /**
   * Return a checker that will be called to determine what constraints should be applied
   * when a user requests or solves a challenge (captchas, push challenges, etc).
   *
   * @return a {@link ChallengeConstraintChecker} controlled by the spam filter
   */
  ChallengeConstraintChecker getChallengeConstraintChecker();

  /**
   * Return a checker that will be called to determine if a user is allowed to use their
   * registration recovery password to re-register
   *
   * @return a {@link RegistrationRecoveryChecker} controlled by the spam filter
   */
  RegistrationRecoveryChecker getRegistrationRecoveryChecker();

  /**
   * Return a function that will be used to lazily fetch the captcha client for a specified scheme. This is to avoid
   * initialization issues with the spam filter if eagerly fetched.
   *
   * @return a {@link Function} that takes the scheme and returns a {@link CaptchaClient}. Returns null if no captcha
   * client for the scheme exists
   */
  Function<String, CaptchaClient> getCaptchaClientSupplier();
}
