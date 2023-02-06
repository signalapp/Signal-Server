/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Pair;

class RegistrationLockVerificationManagerTest {

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final ClientPresenceManager clientPresenceManager = mock(ClientPresenceManager.class);
  private final ExternalServiceCredentialsGenerator backupServiceCredentialsGeneraor = mock(
      ExternalServiceCredentialsGenerator.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = new RegistrationLockVerificationManager(
      accountsManager, clientPresenceManager, backupServiceCredentialsGeneraor, rateLimiters);

  private final RateLimiter pinLimiter = mock(RateLimiter.class);

  private Account account;
  private StoredRegistrationLock existingRegistrationLock;

  @BeforeEach
  void setUp() {
    when(rateLimiters.getPinLimiter()).thenReturn(pinLimiter);
    when(backupServiceCredentialsGeneraor.generateForUuid(any()))
        .thenReturn(mock(ExternalServiceCredentials.class));

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn("+18005551212");
    existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(account.getRegistrationLock()).thenReturn(existingRegistrationLock);
  }

  @ParameterizedTest
  @EnumSource
  void testErrors(RegistrationLockError error) throws Exception {

    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(true);

    final String submittedRegistrationLock = "reglock";

    final Pair<Class<? extends Exception>, Consumer<Exception>> exceptionType = switch (error) {
      case MISMATCH -> {
        when(existingRegistrationLock.verify(submittedRegistrationLock)).thenReturn(false);
        yield new Pair<>(WebApplicationException.class, e -> {
          if (e instanceof WebApplicationException wae) {
            assertEquals(RegistrationLockVerificationManager.FAILURE_HTTP_STATUS, wae.getResponse().getStatus());
          } else {
            fail("Exception was not of expected type");
          }
        });
      }
      case RATE_LIMITED -> {
        when(existingRegistrationLock.verify(any())).thenReturn(true);
        doThrow(RateLimitExceededException.class).when(pinLimiter).validate(anyString());
        yield new Pair<>(RateLimitExceededException.class, ignored -> {
        });
      }
    };

    final Exception e = assertThrows(exceptionType.first(), () ->
        registrationLockVerificationManager.verifyRegistrationLock(account, submittedRegistrationLock));

    exceptionType.second().accept(e);
  }

  @ParameterizedTest
  @MethodSource
  void testSuccess(final boolean requiresClientRegistrationLock, @Nullable final String submittedRegistrationLock) {

    when(existingRegistrationLock.requiresClientRegistrationLock())
        .thenReturn(requiresClientRegistrationLock);
    when(existingRegistrationLock.verify(submittedRegistrationLock)).thenReturn(true);

    assertDoesNotThrow(
        () -> registrationLockVerificationManager.verifyRegistrationLock(account, submittedRegistrationLock));
  }

  static Stream<Arguments> testSuccess() {
    return Stream.of(
        Arguments.of(false, null),
        Arguments.of(true, null),
        Arguments.of(false, "reglock"),
        Arguments.of(true, "reglock")
    );
  }

}
