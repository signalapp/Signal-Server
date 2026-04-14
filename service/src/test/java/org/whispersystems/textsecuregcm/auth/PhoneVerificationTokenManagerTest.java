/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.grpc.Status;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
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

  private PhoneVerificationTokenManager phoneVerificationTokenManager;

  private static final String PHONE_NUMBER = PhoneNumberUtil.getInstance()
      .format(PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final UUID PHONE_NUMBER_IDENTIFIER = UUID.randomUUID();

  record PhoneVerificationRequest(String sessionId, byte[] recoveryPassword) implements org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest {
    static PhoneVerificationRequest forSessionId(final byte[] sessionId) {
      return new PhoneVerificationRequest(Base64.getUrlEncoder().encodeToString(sessionId), null);
    }

    static PhoneVerificationRequest forRecoveryPassword(final byte[] recoveryPassword) {
      return new PhoneVerificationRequest(null, recoveryPassword);
    }
  }

  @BeforeEach
  void setUp() {
    final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
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
          .thenReturn(CompletableFuture.completedFuture(Optional.of(
              new RegistrationServiceSession(sessionId, PHONE_NUMBER, true, null, null, null, 0))));

      assertDoesNotThrow(() -> phoneVerificationTokenManager.verify(mock(ContainerRequestContext.class),
          PHONE_NUMBER,
          PhoneVerificationRequest.forSessionId(sessionId)));
    }

    @Test
    void verifySessionNotFound() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

      assertThrows(NotAuthorizedException.class, () -> phoneVerificationTokenManager.verify(mock(ContainerRequestContext.class),
          PHONE_NUMBER,
          PhoneVerificationRequest.forSessionId(sessionId)));
    }

    @Test
    void verifyNumberMismatch() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(
              new RegistrationServiceSession(sessionId, PHONE_NUMBER + "0", true, null, null, null, 0))));

      assertThrows(BadRequestException.class, () -> phoneVerificationTokenManager.verify(mock(ContainerRequestContext.class),
          PHONE_NUMBER,
          PhoneVerificationRequest.forSessionId(sessionId)));
    }

    @Test
    void verifySessionNotVerified() {
      final byte[] sessionId = TestRandomUtil.nextBytes(16);

      when(registrationServiceClient.getSession(eq(sessionId), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(
              new RegistrationServiceSession(sessionId, PHONE_NUMBER, false, null, null, null, 0))));

      assertThrows(NotAuthorizedException.class, () -> phoneVerificationTokenManager.verify(mock(ContainerRequestContext.class),
          PHONE_NUMBER,
          PhoneVerificationRequest.forSessionId(sessionId)));
    }

    @ParameterizedTest
    @MethodSource
    void verifyRegistrationServiceClientException(final Throwable registrationServiceClientException,
        final Class<Throwable> expectedExceptionClass) throws ExecutionException, InterruptedException, TimeoutException {

      @SuppressWarnings("unchecked") final CompletableFuture<Optional<RegistrationServiceSession>> mockFuture
          = mock(CompletableFuture.class);

      when(mockFuture.get(anyLong(), any())).thenThrow(registrationServiceClientException);
      when(registrationServiceClient.getSession(any(), any())).thenReturn(mockFuture);

      assertThrows(expectedExceptionClass, () -> phoneVerificationTokenManager.verify(mock(ContainerRequestContext.class),
          PHONE_NUMBER,
          PhoneVerificationRequest.forSessionId(TestRandomUtil.nextBytes(16))));
    }

    private static List<Arguments> verifyRegistrationServiceClientException() {
      return List.of(
          Arguments.arguments(new ExecutionException(Status.INVALID_ARGUMENT.asRuntimeException()), BadRequestException.class),
          Arguments.arguments(new ExecutionException(Status.RESOURCE_EXHAUSTED.asRuntimeException()), ServerErrorException.class),
          Arguments.arguments(new CancellationException(), ServerErrorException.class),
          Arguments.arguments(new TimeoutException(), ServerErrorException.class)
      );
    }
  }

  @Nested
  class PasswordBasedVerification {

    @Test
    void verify() {
      final ContainerRequestContext containerRequestContext = mock(ContainerRequestContext.class);
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(containerRequestContext, PHONE_NUMBER))
          .thenReturn(true);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(CompletableFuture.completedFuture(true));

      assertDoesNotThrow(() -> phoneVerificationTokenManager.verify(containerRequestContext,
          PHONE_NUMBER,
          PhoneVerificationRequest.forRecoveryPassword(recoveryPassword)));
    }

    @Test
    void verifyRecoveryCheckerDeclined() {
      final ContainerRequestContext containerRequestContext = mock(ContainerRequestContext.class);
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(containerRequestContext, PHONE_NUMBER))
          .thenReturn(false);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(CompletableFuture.completedFuture(true));

      assertThrows(ForbiddenException.class, () -> phoneVerificationTokenManager.verify(containerRequestContext,
          PHONE_NUMBER,
          PhoneVerificationRequest.forRecoveryPassword(recoveryPassword)));
    }

    @Test
    void verifyRecoveryPasswordNotVerified() {
      final ContainerRequestContext containerRequestContext = mock(ContainerRequestContext.class);
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(containerRequestContext, PHONE_NUMBER))
          .thenReturn(true);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(CompletableFuture.completedFuture(false));

      assertThrows(ForbiddenException.class, () -> phoneVerificationTokenManager.verify(containerRequestContext,
          PHONE_NUMBER,
          PhoneVerificationRequest.forRecoveryPassword(recoveryPassword)));
    }

    @ParameterizedTest
    @MethodSource
    void verifyRecoveryPasswordManagerException(final Throwable recoveryPasswordManagerException)
        throws ExecutionException, InterruptedException, TimeoutException {

      final ContainerRequestContext containerRequestContext = mock(ContainerRequestContext.class);
      final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(containerRequestContext, PHONE_NUMBER))
          .thenReturn(true);

      @SuppressWarnings("unchecked") final CompletableFuture<Boolean> mockFuture = mock(CompletableFuture.class);
      when(mockFuture.get(anyLong(), any())).thenThrow(recoveryPasswordManagerException);

      when(registrationRecoveryPasswordsManager.verify(PHONE_NUMBER_IDENTIFIER, recoveryPassword))
          .thenReturn(mockFuture);

      assertThrows(ServerErrorException.class, () -> phoneVerificationTokenManager.verify(containerRequestContext,
          PHONE_NUMBER,
          PhoneVerificationRequest.forRecoveryPassword(recoveryPassword)));
    }

    private static List<Throwable> verifyRecoveryPasswordManagerException() {
      return List.of(new ExecutionException(new RuntimeException()), new TimeoutException());
    }
  }
}
