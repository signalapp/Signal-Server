/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.util.Util;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class StoredRegistrationLock {

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

  public StoredRegistrationLock(Optional<String> registrationLock, Optional<String> registrationLockSalt, long lastSeen) {
    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
    this.lastSeen             = lastSeen;
  }

  public boolean requiresClientRegistrationLock() {
    boolean hasTimeRemaining = getTimeRemaining() >= 0;
    return hasLockAndSalt() && hasTimeRemaining;
  }

  public boolean needsFailureCredentials() {
    return hasLockAndSalt();
  }

  public long getTimeRemaining() {
    return TimeUnit.DAYS.toMillis(7) - timeSinceLastSeen();
  }

  public boolean verify(@Nullable String clientRegistrationLock) {
    if (hasLockAndSalt() && Util.nonEmpty(clientRegistrationLock)) {
      AuthenticationCredentials credentials = new AuthenticationCredentials(registrationLock.get(), registrationLockSalt.get());
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
