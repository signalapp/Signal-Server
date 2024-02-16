/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import io.dropwizard.lifecycle.Managed;
import org.whispersystems.textsecuregcm.storage.ReportedMessageListener;
import javax.ws.rs.container.ContainerRequestFilter;
import java.io.IOException;

/**
 * A spam filter provides various checkers and listeners to detect and respond to patterns of spam and fraud.
 * <p/>
 * Spam filters are managed components that are generally loaded dynamically via a {@link java.util.ServiceLoader}.
 * Their {@link #configure(String)} method will be called prior to be adding to the server's pool of {@link Managed}
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
   * @throws IOException if the filter could not read its configuration source for any reason
   */
  void configure(String environmentName) throws IOException;

  /**
   * Builds a spam report token provider. This will generate tokens used by the spam reporting system.
   *
   * @return the configured spam report token provider.
   */
  ReportSpamTokenProvider getReportSpamTokenProvider();

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
}
