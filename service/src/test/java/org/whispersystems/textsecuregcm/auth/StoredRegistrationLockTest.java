package org.whispersystems.textsecuregcm.auth;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import javax.swing.text.html.Option;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.auth.StoredRegistrationLock.REGISTRATION_LOCK_EXPIRATION_DAYS;

public class StoredRegistrationLockTest {
  @ParameterizedTest
  @MethodSource
  void getStatus(final Optional<String> registrationLock, final Optional<String> salt, final long lastSeen,
      final StoredRegistrationLock.Status expectedStatus) {
    final StoredRegistrationLock storedLock = new StoredRegistrationLock(registrationLock, salt, Instant.ofEpochMilli(lastSeen));

    assertEquals(expectedStatus, storedLock.getStatus());
  }

  private static Stream<Arguments> getStatus() {
    return Stream.of(
        Arguments.of(Optional.of("registrationLock"), Optional.of("salt"), System.currentTimeMillis() - Duration.ofDays(1).toMillis(), StoredRegistrationLock.Status.REQUIRED),
        Arguments.of(Optional.empty(), Optional.empty(), 0L, StoredRegistrationLock.Status.ABSENT),
        Arguments.of(Optional.of("registrationLock"), Optional.of("salt"), System.currentTimeMillis() - REGISTRATION_LOCK_EXPIRATION_DAYS.toMillis(), StoredRegistrationLock.Status.EXPIRED)
    );
  }

}
