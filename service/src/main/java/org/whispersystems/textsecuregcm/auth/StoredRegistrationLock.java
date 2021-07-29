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

  public StoredRegistrationLock(Optional<String> registrationLock, Optional<String> registrationLockSalt, long lastSeen) {
    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
    this.lastSeen             = lastSeen;
  }

  public boolean requiresClientRegistrationLock() {
    return registrationLock.isPresent() && registrationLockSalt.isPresent() && System.currentTimeMillis() - lastSeen < TimeUnit.DAYS.toMillis(7);
  }

  public boolean needsFailureCredentials() {
    return registrationLock.isPresent() && registrationLockSalt.isPresent();
  }

  public long getTimeRemaining() {
    return TimeUnit.DAYS.toMillis(7) - (System.currentTimeMillis() - lastSeen);
  }

  public boolean verify(@Nullable String clientRegistrationLock) {
    if (Util.isEmpty(clientRegistrationLock)) {
      return false;
    }

    if (registrationLock.isPresent() && registrationLockSalt.isPresent() && !Util.isEmpty(clientRegistrationLock)) {
      return new AuthenticationCredentials(registrationLock.get(), registrationLockSalt.get()).verify(clientRegistrationLock);
    } else {
      return false;
    }
  }

  @VisibleForTesting
  public StoredRegistrationLock forTime(long timestamp) {
    return new StoredRegistrationLock(registrationLock, registrationLockSalt, timestamp);
  }
}
