/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.configuration.UsernameConfiguration;
import org.whispersystems.textsecuregcm.storage.UsernameNotAvailableException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class UsernameGenerator {
  /**
   * Nicknames
   * <list>
   *   <li> do not start with a number </li>
   *   <li> are alphanumeric or underscores only </li>
   *   <li> have minimum length 3 </li>
   *   <li> have maximum length 32 </li>
   * </list>
   *
   * Usernames typically consist of a nickname and an integer discriminator
   */
  public static final Pattern NICKNAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]{2,31}$");
  public static final String SEPARATOR = ".";

  private static final Counter USERNAME_NOT_AVAILABLE_COUNTER = Metrics.counter(name(UsernameGenerator.class, "usernameNotAvailable"));
  private static final DistributionSummary DISCRIMINATOR_ATTEMPT_COUNTER = Metrics.summary(name(UsernameGenerator.class, "discriminatorAttempts"));

  private final int initialWidth;
  private final int discriminatorMaxWidth;
  private final int attemptsPerWidth;
  private final Duration reservationTtl;

  public UsernameGenerator(UsernameConfiguration configuration) {
    this(configuration.getDiscriminatorInitialWidth(),
        configuration.getDiscriminatorMaxWidth(),
        configuration.getAttemptsPerWidth(),
        configuration.getReservationTtl());
  }

  @VisibleForTesting
  public UsernameGenerator(int initialWidth, int discriminatorMaxWidth, int attemptsPerWidth, final Duration reservationTtl) {
    this.initialWidth = initialWidth;
    this.discriminatorMaxWidth = discriminatorMaxWidth;
    this.attemptsPerWidth = attemptsPerWidth;
    this.reservationTtl = reservationTtl;
  }

  /**
   * Generate a username with a random discriminator
   *
   * @param nickname The string nickname
   * @param usernameAvailableFun A {@link Predicate} that returns true if the provided username is available
   * @return The nickname appended with a random discriminator
   * @throws UsernameNotAvailableException if we failed to find a nickname+discriminator pair that was available
   */
  public String generateAvailableUsername(final String nickname, final Predicate<String> usernameAvailableFun) throws UsernameNotAvailableException {
    int rangeMin = 1;
    int rangeMax = IntMath.pow(10, initialWidth);
    int totalMax = IntMath.pow(10, discriminatorMaxWidth);
    int attempts = 0;
    while (rangeMax <= totalMax) {
      // check discriminators of the current width up to attemptsPerWidth times
      for (int i = 0; i < attemptsPerWidth; i++) {
        int discriminator = ThreadLocalRandom.current().nextInt(rangeMin, rangeMax);
        String username = fromParts(nickname, discriminator);
        attempts++;
        if (usernameAvailableFun.test(username)) {
          DISCRIMINATOR_ATTEMPT_COUNTER.record(attempts);
          return username;
        }
      }

      // update the search range to look for numbers of one more digit
      // than the previous iteration
      rangeMin = rangeMax;
      rangeMax *= 10;
    }
    USERNAME_NOT_AVAILABLE_COUNTER.increment();
    throw new UsernameNotAvailableException();
  }

  /**
   * Strips the discriminator from a username, if it is present
   *
   * @param username the string username
   * @return the nickname prefix of the username
   */
  public static String extractNickname(final String username) {
    int sep = username.indexOf(SEPARATOR);
    return sep == -1 ? username : username.substring(0, sep);
  }

  /**
   * Generate a username from a nickname and discriminator
   */
  public String fromParts(final String nickname, final int discriminator) throws IllegalArgumentException {
    if (!isValidNickname(nickname)) {
      throw new IllegalArgumentException("Invalid nickname " + nickname);
    }
    // zero pad discriminators less than the discriminator initial width
    return String.format("%s%s%0" + initialWidth + "d",  nickname, SEPARATOR, discriminator);
  }

  public Duration getReservationTtl() {
    return reservationTtl;
  }

  public static boolean isValidNickname(final String nickname) {
    return StringUtils.isNotBlank(nickname) && NICKNAME_PATTERN.matcher(nickname).matches();
  }

  /**
   * Checks if the username consists of a valid nickname followed by an integer discriminator
   *
   * @param username string username to check
   * @return true if the username is in standard form
   */
  public static boolean isStandardFormat(final String username) {
    if (username == null) {
      return false;
    }
    int sep = username.indexOf(SEPARATOR);
    if (sep == -1) {
      return false;
    }
    final String nickname = username.substring(0, sep);
    if (!isValidNickname(nickname)) {
      return false;
    }

    try {
      int discriminator = Integer.parseInt(username.substring(sep + 1));
      return discriminator > 0;
    } catch (NumberFormatException e) {
      return false;
    }
  }

}
