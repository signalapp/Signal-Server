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

import jakarta.ws.rs.WebApplicationException;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
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
  private final DisconnectionRequestManager disconnectionRequestManager = mock(DisconnectionRequestManager.class);
  private final ExternalServiceCredentialsGenerator svr2CredentialsGenerator = mock(
      ExternalServiceCredentialsGenerator.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = new RegistrationLockVerificationManager(
      accountsManager, disconnectionRequestManager, svr2CredentialsGenerator, registrationRecoveryPasswordsManager,
      pushNotificationManager, rateLimiters);

  private final RateLimiter pinLimiter = mock(RateLimiter.class);

  private Account account;
  private StoredRegistrationLock existingRegistrationLock;

  @BeforeEach
  void setUp() {
    clearInvocations(pushNotificationManager);
    when(rateLimiters.getPinLimiter()).thenReturn(pinLimiter);
    when(svr2CredentialsGenerator.generateForUuid(any()))
        .thenReturn(mock(ExternalServiceCredentials.class));

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

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
  void testErrors(RegistrationLockError error,
      PhoneVerificationRequest.VerificationType verificationType,
      @Nullable String clientRegistrationLock,
      boolean alreadyLocked) throws Exception {

    when(existingRegistrationLock.getStatus()).thenReturn(StoredRegistrationLock.Status.REQUIRED);
    when(account.hasLockedCredentials()).thenReturn(alreadyLocked);
    doThrow(new NotPushRegisteredException()).when(pushNotificationManager).sendAttemptLoginNotification(any(), any());

    final Pair<Class<? extends Exception>, Consumer<Exception>> exceptionType = switch (error) {
      case MISMATCH -> {
        when(existingRegistrationLock.verify(clientRegistrationLock)).thenReturn(false);
        yield new Pair<>(WebApplicationException.class, e -> {
          if (e instanceof WebApplicationException wae) {
            assertEquals(RegistrationLockVerificationManager.FAILURE_HTTP_STATUS, wae.getResponse().getStatus());
            if (!verificationType.equals(PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD) || clientRegistrationLock != null) {
              verify(registrationRecoveryPasswordsManager).remove(account.getIdentifier(IdentityType.PNI));
            } else {
              verify(registrationRecoveryPasswordsManager, never()).remove(any());
            }
            verify(disconnectionRequestManager).requestDisconnection(account.getUuid(), List.of(Device.PRIMARY_ID));
            try {
              verify(pushNotificationManager).sendAttemptLoginNotification(any(), eq("failedRegistrationLock"));
            } catch (final NotPushRegisteredException ignored) {
            }
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
          } catch (final NotPushRegisteredException ignored2) {
          }

          verify(registrationRecoveryPasswordsManager, never()).remove(any());
          verify(disconnectionRequestManager, never()).requestDisconnection(any(), any());
        });
      }
    };

    final Exception e = assertThrows(exceptionType.first(), () ->
        registrationLockVerificationManager.verifyRegistrationLock(account, clientRegistrationLock,
            "Signal-Android/4.68.3", RegistrationLockVerificationManager.Flow.REGISTRATION,
            verificationType));

    exceptionType.second().accept(e);
  }

  static Stream<Arguments> testErrors() {
    return Stream.of(
        Arguments.of(RegistrationLockError.MISMATCH, PhoneVerificationRequest.VerificationType.SESSION, "reglock", true),
        Arguments.of(RegistrationLockError.MISMATCH, PhoneVerificationRequest.VerificationType.SESSION, "reglock", false),
        Arguments.of(RegistrationLockError.MISMATCH, PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD, "reglock", false),
        Arguments.of(RegistrationLockError.MISMATCH, PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD, null, false),
        Arguments.of(RegistrationLockError.RATE_LIMITED, PhoneVerificationRequest.VerificationType.SESSION, "reglock", false)
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
    verify(registrationRecoveryPasswordsManager, never()).remove(any());
    verify(disconnectionRequestManager, never()).requestDisconnection(any(), any());
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
