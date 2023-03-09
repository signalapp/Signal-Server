/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.util.Util;

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

  private final long             lastSeen;

  /**
   * @return milliseconds since the last time the account was seen.
   */
  private long timeSinceLastSeen() {
    return System.currentTimeMillis() - lastSeen;
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

  public StoredRegistrationLock(Optional<String> registrationLock, Optional<String> registrationLockSalt, long lastSeen) {
    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
    this.lastSeen             = lastSeen;
  }

  private boolean hasTimeRemaining() {
    return getTimeRemaining() >= 0;
  }

  public Status getStatus() {
    if (!isPresent()) {
      return Status.ABSENT;
    }
    if (hasTimeRemaining()) {
      return Status.REQUIRED;
    }
    return Status.EXPIRED;
  }

  public boolean needsFailureCredentials() {
    return hasLockAndSalt();
  }

  public long getTimeRemaining() {
    return REGISTRATION_LOCK_EXPIRATION_DAYS.toMillis() - timeSinceLastSeen();
  }

  public boolean verify(@Nullable String clientRegistrationLock) {
    if (hasLockAndSalt() && Util.nonEmpty(clientRegistrationLock)) {
      SaltedTokenHash credentials = new SaltedTokenHash(registrationLock.get(), registrationLockSalt.get());
      return credentials.verify(clientRegistrationLock);
    } else {
      return false;
    }
  }

  @VisibleForTesting
  public StoredRegistrationLock forTime(long timestamp) {
    return new StoredRegistrationLock(registrationLock, registrationLockSalt, timestamp);
  }
}
