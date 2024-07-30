/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class StoredRegistrationLock {
  public enum Status {
    REQUIRED,
    EXPIRED,
    ABSENT
  }

  @VisibleForTesting
  static final Duration REGISTRATION_LOCK_EXPIRATION_DAYS = Duration.ofDays(7);

  private final Optional<String> registrationLock;

  private final Optional<String> registrationLockSalt;

  private final Instant lastSeen;

  /**
   * @return milliseconds since the last time the account was seen.
   */
  private long timeSinceLastSeen() {
    return System.currentTimeMillis() - lastSeen.toEpochMilli();
  }

  /**
   * @return true if the registration lock and salt are both set.
   */
  private boolean hasLockAndSalt() {
    return registrationLock.isPresent() && registrationLockSalt.isPresent();
  }

  public boolean isPresent() {
    return hasLockAndSalt();
  }

  public StoredRegistrationLock(Optional<String> registrationLock, Optional<String> registrationLockSalt, Instant lastSeen) {
    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
    this.lastSeen             = lastSeen;
  }

  public Status getStatus() {
    if (!isPresent()) {
      return Status.ABSENT;
    }
    if (getTimeRemaining().toMillis() > 0) {
      return Status.REQUIRED;
    }
    return Status.EXPIRED;
  }

  public boolean needsFailureCredentials() {
    return hasLockAndSalt();
  }

  public Duration getTimeRemaining() {
    return REGISTRATION_LOCK_EXPIRATION_DAYS.minus(timeSinceLastSeen(), ChronoUnit.MILLIS);
  }

  public boolean verify(@Nullable String clientRegistrationLock) {
    if (hasLockAndSalt() && StringUtils.isNotEmpty(clientRegistrationLock)) {
      SaltedTokenHash credentials = new SaltedTokenHash(registrationLock.get(), registrationLockSalt.get());
      return credentials.verify(clientRegistrationLock);
    } else {
      return false;
    }
  }
}
