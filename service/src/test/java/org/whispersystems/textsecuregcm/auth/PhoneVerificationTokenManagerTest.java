/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.grpc.Status;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.spam.RegistrationRecoveryChecker;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class PhoneVerificationTokenManagerTest {

  private RegistrationServiceClient registrationServiceClient;
  private RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private RegistrationRecoveryChecker registrationRecoveryChecker;
  private PhoneNumberIdentifiers phoneNumberIdentifiers;

  private PhoneVerificationTokenManager phoneVerificationTokenManager;

  private static final String PHONE_NUMBER = PhoneNumberUtil.getInstance()
      .format(PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final UUID PHONE_NUMBER_IDENTIFIER = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(PHONE_NUMBER))
        .thenReturn(CompletableFuture.completedFuture(PHONE_NUMBER_IDENTIFIER));

    registrationServiceClient = mock(RegistrationServiceClient.class);
    registrationRecoveryPasswordsManager = mock(RegistrationRecoveryPasswordsManager.class);
    registrationRecoveryChecker = mock(RegistrationRecoveryChecker.class);

    phoneVerificationTokenManager = new PhoneVerificationTokenManager(phoneNumberIdentifiers,
        registrationServiceClient,
        registrationRecoveryPasswordsManager,
        registrationRecoveryChecker);
  }

  @Nested
  class SessionBasedVerification {

    @Test
    void verify() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(Optional.of(
              new RegistrationServiceSession(sessionId, PHONE_NUMBER, true, null, null, null, 0)));

      assertDoesNotThrow(() -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          sessionId,
          null));
    }

    @Test
    void verifySessionNotFound() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(Optional.empty());

      assertThrows(UnverifiedRegistrationSessionException.class, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          sessionId,
          null));
    }

    @Test
    void verifyNumberMismatch() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(Optional.of(
              new RegistrationServiceSession(sessionId, PHONE_NUMBER + "0", true, null, null, null, 0)));

      assertThrows(InvalidRegistrationSessionException.class, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          sessionId,
          null));
    }

    @Test
    void verifySessionNotVerified() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(Optional.of(
              new RegistrationServiceSession(sessionId, PHONE_NUMBER, false, null, null, null, 0)));

      assertThrows(UnverifiedRegistrationSessionException.class, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          sessionId,
          null));
    }

    @ParameterizedTest
    @MethodSource
    void verifyRegistrationServiceClientException(final Throwable registrationServiceClientException,
        final Class<Throwable> expectedExceptionClass) {

      when(registrationServiceClient.getSession(any(), any())).thenThrow(registrationServiceClientException);

      assertThrows(expectedExceptionClass, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          TestRandomUtil.nextBytes(16),
          null));
    }

    private static List<Arguments> verifyRegistrationServiceClientException() {
      return List.of(
          Arguments.arguments(Status.INVALID_ARGUMENT.asRuntimeException(), InvalidRegistrationSessionException.class),
          Arguments.arguments(Status.RESOURCE_EXHAUSTED.asRuntimeException(), IOException.class),
          Arguments.arguments(Status.DEADLINE_EXCEEDED.asRuntimeException(), IOException.class)
      );
    }
  }

  @Nested
  class PasswordBasedVerification {

    @Test
    void verify() {
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(PHONE_NUMBER, null, null, null))
          .thenReturn(true);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(true);

      assertDoesNotThrow(() -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          null,
          recoveryPassword));
    }

    @Test
    void verifyRecoveryCheckerDeclined() {
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(PHONE_NUMBER, null, null, null))
          .thenReturn(false);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(true);

      assertThrows(RecoveryPasswordVerificationFailedException.class, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          null,
          recoveryPassword));
    }

    @Test
    void verifyRecoveryPasswordNotVerified() {
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(PHONE_NUMBER, null, null, null))
          .thenReturn(true);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(false);

      assertThrows(RecoveryPasswordVerificationFailedException.class, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          null,
          recoveryPassword));
    }

    @ParameterizedTest
    @MethodSource
    void verifyPhoneNumberIdentifiersException(final Throwable phoneNumberIdentifiersException) {

      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(PHONE_NUMBER, null, null, null))
          .thenReturn(true);

      when(phoneNumberIdentifiers.getPhoneNumberIdentifier(PHONE_NUMBER))
          .thenReturn(CompletableFuture.failedFuture(phoneNumberIdentifiersException));

      assertThrows(IOException.class, () -> phoneVerificationTokenManager.verify(
          PHONE_NUMBER,
          null,
          null,
          null,
          null,
          recoveryPassword));
    }

    private static List<Throwable> verifyPhoneNumberIdentifiersException() {
      return List.of(new ExecutionException(new RuntimeException()), new TimeoutException());
    }
  }
}
