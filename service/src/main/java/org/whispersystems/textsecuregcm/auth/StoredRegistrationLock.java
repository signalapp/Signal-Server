package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.util.Util;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class StoredRegistrationLock {

  private final Optional<String> registrationLock;

  private final Optional<String> registrationLockSalt;

  private final Optional<String> deprecatedPin;

  private final long             lastSeen;

  public StoredRegistrationLock(Optional<String> registrationLock, Optional<String> registrationLockSalt, Optional<String> deprecatedPin, long lastSeen) {
    this.registrationLock     = registrationLock;
    this.registrationLockSalt = registrationLockSalt;
    this.deprecatedPin        = deprecatedPin;
    this.lastSeen             = lastSeen;
  }

  public boolean requiresClientRegistrationLock() {
    return ((registrationLock.isPresent() && registrationLockSalt.isPresent())  || deprecatedPin.isPresent()) && System.currentTimeMillis() - lastSeen < TimeUnit.DAYS.toMillis(7);
  }

  public boolean needsFailureCredentials() {
    return registrationLock.isPresent() && registrationLockSalt.isPresent();
  }

  public long getTimeRemaining() {
    return TimeUnit.DAYS.toMillis(7) - (System.currentTimeMillis() - lastSeen);
  }

  public boolean verify(@Nullable String clientRegistrationLock, @Nullable String clientDeprecatedPin) {
    if (Util.isEmpty(clientRegistrationLock) && Util.isEmpty(clientDeprecatedPin)) {
      return false;
    }

    if (registrationLock.isPresent() && registrationLockSalt.isPresent() && !Util.isEmpty(clientRegistrationLock)) {
      return new AuthenticationCredentials(registrationLock.get(), registrationLockSalt.get()).verify(clientRegistrationLock);
    } else if (deprecatedPin.isPresent() && !Util.isEmpty(clientDeprecatedPin)) {
      return MessageDigest.isEqual(deprecatedPin.get().getBytes(), clientDeprecatedPin.getBytes());
    } else {
      return false;
    }
  }

  @VisibleForTesting
  public StoredRegistrationLock forTime(long timestamp) {
    return new StoredRegistrationLock(registrationLock, registrationLockSalt, deprecatedPin, timestamp);
  }
}
