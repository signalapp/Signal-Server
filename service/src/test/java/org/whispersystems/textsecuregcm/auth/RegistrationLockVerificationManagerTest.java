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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.Pair;

class RegistrationLockVerificationManagerTest {

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final ClientPresenceManager clientPresenceManager = mock(ClientPresenceManager.class);
  private final ExternalServiceCredentialsGenerator backupServiceCredentialsGeneraor = mock(
      ExternalServiceCredentialsGenerator.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private static PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = new RegistrationLockVerificationManager(
      accountsManager, clientPresenceManager, backupServiceCredentialsGeneraor, registrationRecoveryPasswordsManager, pushNotificationManager, rateLimiters);

  private final RateLimiter pinLimiter = mock(RateLimiter.class);

  private Account account;
  private StoredRegistrationLock existingRegistrationLock;

  @BeforeEach
  void setUp() {
    clearInvocations(pushNotificationManager);
    when(rateLimiters.getPinLimiter()).thenReturn(pinLimiter);
    when(backupServiceCredentialsGeneraor.generateForUuid(any()))
        .thenReturn(mock(ExternalServiceCredentials.class));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.MASTER_ID);

    AccountsHelper.setupMockUpdate(accountsManager);

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn("+18005551212");
    when(account.getDevices()).thenReturn(List.of(device));

    existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(account.getRegistrationLock()).thenReturn(existingRegistrationLock);
  }

  @ParameterizedTest
  @MethodSource
  void testErrors(RegistrationLockError error, boolean alreadyLocked) throws Exception {

    when(existingRegistrationLock.getStatus()).thenReturn(StoredRegistrationLock.Status.REQUIRED);
    when(account.hasLockedCredentials()).thenReturn(alreadyLocked);
    doThrow(new NotPushRegisteredException()).when(pushNotificationManager).sendAttemptLoginNotification(any(), any());

    final String submittedRegistrationLock = "reglock";

    final Pair<Class<? extends Exception>, Consumer<Exception>> exceptionType = switch (error) {
      case MISMATCH -> {
        when(existingRegistrationLock.verify(submittedRegistrationLock)).thenReturn(false);
        yield new Pair<>(WebApplicationException.class, e -> {
          if (e instanceof WebApplicationException wae) {
            assertEquals(RegistrationLockVerificationManager.FAILURE_HTTP_STATUS, wae.getResponse().getStatus());
            verify(registrationRecoveryPasswordsManager).removeForNumber(account.getNumber());
            verify(clientPresenceManager).disconnectAllPresences(account.getUuid(), List.of(Device.MASTER_ID));
            try {
              verify(pushNotificationManager).sendAttemptLoginNotification(any(), eq("failedRegistrationLock"));
            } catch (NotPushRegisteredException npre) {}
            if (alreadyLocked) {
              verify(account, never()).lockAuthTokenHash();
            } else {
              verify(account).lockAuthTokenHash();
            }
          } else {
            fail("Exception was not of expected type");
          }
        });
      }
      case RATE_LIMITED -> {
        when(existingRegistrationLock.verify(any())).thenReturn(true);
        doThrow(RateLimitExceededException.class).when(pinLimiter).validate(anyString());
        yield new Pair<>(RateLimitExceededException.class, ignored -> {
          verify(account, never()).lockAuthTokenHash();
          try {
            verify(pushNotificationManager, never()).sendAttemptLoginNotification(any(), eq("failedRegistrationLock"));
          } catch (NotPushRegisteredException npre) {}
          verify(registrationRecoveryPasswordsManager, never()).removeForNumber(account.getNumber());
          verify(clientPresenceManager, never()).disconnectAllPresences(account.getUuid(), List.of(Device.MASTER_ID));
        });
      }
    };

    final Exception e = assertThrows(exceptionType.first(), () ->
        registrationLockVerificationManager.verifyRegistrationLock(account, submittedRegistrationLock,
            "Signal-Android/4.68.3", RegistrationLockVerificationManager.Flow.REGISTRATION,
            PhoneVerificationRequest.VerificationType.SESSION));

    exceptionType.second().accept(e);
  }

  static Stream<Arguments> testErrors() {
    return Stream.of(
        Arguments.of(RegistrationLockError.MISMATCH, true),
        Arguments.of(RegistrationLockError.MISMATCH, false),
        Arguments.of(RegistrationLockError.RATE_LIMITED, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void testSuccess(final StoredRegistrationLock.Status status, @Nullable final String submittedRegistrationLock) {

    when(existingRegistrationLock.getStatus())
        .thenReturn(status);
    when(existingRegistrationLock.verify(submittedRegistrationLock)).thenReturn(true);

    assertDoesNotThrow(
        () -> registrationLockVerificationManager.verifyRegistrationLock(account, submittedRegistrationLock,
            "Signal-Android/4.68.3", RegistrationLockVerificationManager.Flow.REGISTRATION,
            PhoneVerificationRequest.VerificationType.SESSION));

    verify(account, never()).lockAuthTokenHash();
    verify(registrationRecoveryPasswordsManager, never()).removeForNumber(account.getNumber());
    verify(clientPresenceManager, never()).disconnectAllPresences(account.getUuid(), List.of(Device.MASTER_ID));
  }

  static Stream<Arguments> testSuccess() {
    return Stream.of(
        Arguments.of(StoredRegistrationLock.Status.ABSENT, null),
        Arguments.of(StoredRegistrationLock.Status.EXPIRED, null),
        Arguments.of(StoredRegistrationLock.Status.REQUIRED, null),
        Arguments.of(StoredRegistrationLock.Status.ABSENT, "reglock"),
        Arguments.of(StoredRegistrationLock.Status.EXPIRED, "reglock"),
        Arguments.of(StoredRegistrationLock.Status.REQUIRED, "reglock")
    );
  }

}
