/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import liquibase.util.StringUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.entities.VerificationSessionResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RegistrationServiceSenderExceptionMapper;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceException;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceSenderException;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class VerificationControllerTest {

  private static final long SESSION_EXPIRATION_SECONDS = Duration.ofMinutes(10).toSeconds();

  private static final byte[] SESSION_ID = "session".getBytes(StandardCharsets.UTF_8);
  private static final String NUMBER = "+18005551212";

  private final RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private final VerificationSessionManager verificationSessionManager = mock(VerificationSessionManager.class);
  private final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private final RegistrationCaptchaManager registrationCaptchaManager = mock(RegistrationCaptchaManager.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final Clock clock = Clock.systemUTC();

  private final RateLimiter captchaLimiter = mock(RateLimiter.class);
  private final RateLimiter pushChallengeLimiter = mock(RateLimiter.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .addProvider(new RegistrationServiceSenderExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new VerificationController(registrationServiceClient, verificationSessionManager, pushNotificationManager,
              registrationCaptchaManager, registrationRecoveryPasswordsManager, rateLimiters, clock))
      .build();

  @BeforeEach
  void setUp() {
    when(rateLimiters.getVerificationCaptchaLimiter())
        .thenReturn(captchaLimiter);
    when(rateLimiters.getVerificationPushChallengeLimiter())
        .thenReturn(pushChallengeLimiter);
  }

  @ParameterizedTest
  @MethodSource
  void createSessionUnprocessableRequestJson(final String number, final String pushToken, final String pushTokenType) {

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request();
    try (Response response = request.post(
        Entity.json(unprocessableCreateSessionJson(number, pushToken, pushTokenType)))) {
      assertEquals(400, response.getStatus());
    }

  }

  static Stream<Arguments> createSessionUnprocessableRequestJson() {
    return Stream.of(
        Arguments.of("[]", null, null),
        Arguments.of(String.format("\"%s\"", NUMBER), "some-push-token", "invalid-token-type")
    );
  }

  @ParameterizedTest
  @MethodSource
  void createSessionInvalidRequestJson(final String number, final String pushToken, final String pushTokenType) {

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(createSessionJson(number, pushToken, pushTokenType)))) {
      assertEquals(422, response.getStatus());
    }
  }

  static Stream<Arguments> createSessionInvalidRequestJson() {
    return Stream.of(
        Arguments.of(null, null, null),
        Arguments.of("+1800", null, null),
        Arguments.of(" ", null, null),
        Arguments.of(NUMBER, null, "fcm"),
        Arguments.of(NUMBER, "some-push-token", null)
    );
  }

  @Test
  void createSessionRateLimited() {
    when(registrationServiceClient.createRegistrationSessionSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null, true)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(createSessionJson(NUMBER, null, null)))) {
      assertEquals(429, response.getStatus());
    }
  }

  @Test
  void createSessionRegistrationServiceError() {
    when(registrationServiceClient.createRegistrationSessionSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("expected service error")));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(createSessionJson(NUMBER, null, null)))) {
      assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void createSessionSuccess(final String pushToken, final String pushTokenType,
      final List<VerificationSession.Information> expectedRequestedInformation) {
    when(registrationServiceClient.createRegistrationSessionSession(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new RegistrationServiceSession(SESSION_ID, NUMBER, false, null, null, null,
                    SESSION_EXPIRATION_SECONDS)));
    when(verificationSessionManager.insert(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(createSessionJson(NUMBER, pushToken, pushTokenType)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);
      assertEquals(expectedRequestedInformation, verificationSessionResponse.requestedInformation());
      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertFalse(verificationSessionResponse.verified());
    }
  }

  static Stream<Arguments> createSessionSuccess() {
    return Stream.of(
        Arguments.of(null, null, List.of(VerificationSession.Information.CAPTCHA)),
        Arguments.of("token", "fcm",
            List.of(VerificationSession.Information.PUSH_CHALLENGE, VerificationSession.Information.CAPTCHA))
    );
  }

  @Test
  void patchSessionMalformedId() {
    final String invalidSessionId = "()()()";

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + invalidSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json("{}"))) {
      assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }
  }

  @Test
  void patchSessionNotFound() {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodeSessionId(SESSION_ID))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json("{}"))) {
      assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    }
  }

  @Test
  void patchSessionPushToken() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                new VerificationSession(null, List.of(VerificationSession.Information.CAPTCHA), Collections.emptyList(),
                    false, clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson(null, null, "abcde", "fcm")))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);
      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertEquals(List.of(VerificationSession.Information.PUSH_CHALLENGE, VerificationSession.Information.CAPTCHA),
          updatedSession.requestedInformation());
      assertTrue(updatedSession.submittedInformation().isEmpty());
      assertNotNull(updatedSession.pushChallenge());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertEquals(List.of(VerificationSession.Information.PUSH_CHALLENGE, VerificationSession.Information.CAPTCHA),
          verificationSessionResponse.requestedInformation());
    }
  }

  @Test
  void patchSessionCaptchaRateLimited() throws Exception {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), false,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    doThrow(RateLimitExceededException.class)
        .when(captchaLimiter).validate(anyString());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson("captcha", null, null, null)))) {
      assertEquals(HttpStatus.SC_TOO_MANY_REQUESTS, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void patchSessionPushChallengeRateLimited() throws Exception {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), false,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    doThrow(RateLimitExceededException.class)
        .when(pushChallengeLimiter).validate(anyString());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson(null, "challenge", null, null)))) {
      assertEquals(HttpStatus.SC_TOO_MANY_REQUESTS, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void patchSessionPushChallengeMismatch() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession("challenge", List.of(VerificationSession.Information.PUSH_CHALLENGE),
                Collections.emptyList(), false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));
    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson(null, "mismatched", null, null)))) {
      assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertEquals(List.of(
          VerificationSession.Information.PUSH_CHALLENGE), verificationSessionResponse.requestedInformation());
    }
  }

  @Test
  void patchSessionCaptchaInvalid() throws Exception {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, List.of(VerificationSession.Information.CAPTCHA),
                Collections.emptyList(), false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any()))
        .thenReturn(Optional.of(AssessmentResult.invalid()));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson("captcha", null, null, null)))) {
      assertEquals(HttpStatus.SC_FORBIDDEN, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);

      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertEquals(List.of(VerificationSession.Information.CAPTCHA),
          updatedSession.requestedInformation());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertEquals(List.of(
          VerificationSession.Information.CAPTCHA), verificationSessionResponse.requestedInformation());
    }
  }

  @Test
  void patchSessionPushChallengeAlreadySubmitted() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession("challenge",
                List.of(VerificationSession.Information.CAPTCHA),
                List.of(VerificationSession.Information.PUSH_CHALLENGE), false,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson(null, "challenge", null, null)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);

      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertEquals(List.of(VerificationSession.Information.PUSH_CHALLENGE),
          updatedSession.submittedInformation());
      assertEquals(List.of(VerificationSession.Information.CAPTCHA), updatedSession.requestedInformation());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertEquals(List.of(
          VerificationSession.Information.CAPTCHA), verificationSessionResponse.requestedInformation());
    }
  }

  @Test
  void patchSessionAlreadyVerified() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        true, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession("challenge", List.of(), List.of(), true, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson(null, "challenge", null, null)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.verified());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());

      verify(registrationRecoveryPasswordsManager).removeForNumber(registrationServiceSession.number());
    }
  }

  @Test
  void patchSessionPushChallengeSuccess() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession("challenge",
                List.of(VerificationSession.Information.PUSH_CHALLENGE, VerificationSession.Information.CAPTCHA),
                Collections.emptyList(), false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));
    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson(null, "challenge", null, null)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);

      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertEquals(List.of(VerificationSession.Information.PUSH_CHALLENGE),
          updatedSession.submittedInformation());
      assertTrue(updatedSession.requestedInformation().isEmpty());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void patchSessionCaptchaSuccess() throws Exception {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, List.of(VerificationSession.Information.CAPTCHA),
                Collections.emptyList(), false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any()))
        .thenReturn(Optional.of(new AssessmentResult(true, "1")));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH", Entity.json(updateSessionJson("captcha", null, null, null)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);

      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertEquals(List.of(VerificationSession.Information.CAPTCHA),
          updatedSession.submittedInformation());
      assertTrue(updatedSession.requestedInformation().isEmpty());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void patchSessionPushAndCaptchaSuccess() throws Exception {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession("challenge",
                List.of(VerificationSession.Information.CAPTCHA, VerificationSession.Information.CAPTCHA),
                Collections.emptyList(), false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any()))
        .thenReturn(Optional.of(new AssessmentResult(true, "1")));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH",
        Entity.json(updateSessionJson("captcha", "challenge", null, null)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);

      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertEquals(List.of(VerificationSession.Information.PUSH_CHALLENGE, VerificationSession.Information.CAPTCHA),
          updatedSession.submittedInformation());
      assertTrue(updatedSession.requestedInformation().isEmpty());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void patchSessionTokenUpdatedCaptchaError() throws Exception {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null,
                List.of(VerificationSession.Information.CAPTCHA),
                Collections.emptyList(), false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any()))
        .thenThrow(new IOException("expected service error"));

    when(verificationSessionManager.update(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.method("PATCH",
        Entity.json(updateSessionJson("captcha", null, "token", "fcm")))) {
      assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, response.getStatus());

      final ArgumentCaptor<VerificationSession> verificationSessionArgumentCaptor = ArgumentCaptor.forClass(
          VerificationSession.class);

      verify(verificationSessionManager).update(any(), verificationSessionArgumentCaptor.capture());

      final VerificationSession updatedSession = verificationSessionArgumentCaptor.getValue();
      assertTrue(updatedSession.submittedInformation().isEmpty());
      assertEquals(List.of(VerificationSession.Information.PUSH_CHALLENGE, VerificationSession.Information.CAPTCHA),
          updatedSession.requestedInformation());
      assertNotNull(updatedSession.pushChallenge());
    }
  }

  @Test
  void getSessionMalformedId() {
    final String invalidSessionId = "()()()";

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + invalidSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_UNPROCESSABLE_ENTITY, response.getStatus());
    }
  }

  @Test
  void getSessionNotFound() {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(verificationSessionManager.findForId(encodeSessionId(SESSION_ID)))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodeSessionId(SESSION_ID))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    }

    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                new RegistrationServiceSession(SESSION_ID, NUMBER, false, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));

    request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodeSessionId(SESSION_ID))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    }
  }

  @Test
  void getSessionError() {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodeSessionId(SESSION_ID))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, response.getStatus());
    }
  }

  @Test
  void getSessionSuccess() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                new RegistrationServiceSession(SESSION_ID, NUMBER, false, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(VerificationSession.class))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());
    }
  }

  @Test
  void getSessionSuccessAlreadyVerified() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        true, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(VerificationSession.class))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      verify(registrationRecoveryPasswordsManager).removeForNumber(registrationServiceSession.number());
    }
  }

  @Test
  void requestVerificationCodeAlreadyVerified() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        true, null, null,
        null, SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(registrationServiceSession));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("sms", "android")))) {
      assertEquals(HttpStatus.SC_CONFLICT, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.verified());
    }
  }

  @Test
  void requestVerificationCodeNotAllowedInformationRequested() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(new VerificationSession(null, List.of(
            VerificationSession.Information.CAPTCHA), Collections.emptyList(), false, clock.millis(), clock.millis(),
            registrationServiceSession.expiration()))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("sms", "ios")))) {
      assertEquals(HttpStatus.SC_CONFLICT, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertEquals(List.of(VerificationSession.Information.CAPTCHA),
          verificationSessionResponse.requestedInformation());
    }
  }

  @Test
  void requestVerificationCodeNotAllowed() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, null,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(
                registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), false,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("voice", "android")))) {
      assertEquals(HttpStatus.SC_TOO_MANY_REQUESTS, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertFalse(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void requestVerificationCodeRateLimitExceeded() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null,
        null, SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(
            new CompletionException(new VerificationSessionRateLimitExceededException(registrationServiceSession,
                Duration.ofMinutes(1), true))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("sms", "android")))) {
      assertEquals(HttpStatus.SC_TOO_MANY_REQUESTS, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void requestVerificationCodeSuccess() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null,
        null, SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(registrationServiceSession));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("sms", "android")))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @ParameterizedTest
  @MethodSource
  void requestVerificationCodeExternalServiceRefused(final boolean expectedPermanent, final String expectedReason,
      final RegistrationServiceSenderException exception) {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any()))
        .thenReturn(
            CompletableFuture.failedFuture(new CompletionException(exception)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("voice", "ios")))) {
      assertEquals(HttpStatus.SC_BAD_GATEWAY, response.getStatus());

      final Map<String, Object> responseMap = response.readEntity(Map.class);

      assertEquals(expectedReason, responseMap.get("reason"));
      assertEquals(expectedPermanent, responseMap.get("permanentFailure"));
    }
  }

  static Stream<Arguments> requestVerificationCodeExternalServiceRefused() {
    return Stream.of(
        Arguments.of(true, "illegalArgument", RegistrationServiceSenderException.illegalArgument(true)),
        Arguments.of(true, "providerRejected", RegistrationServiceSenderException.rejected(true)),
        Arguments.of(false, "providerUnavailable", RegistrationServiceSenderException.unknown(false))
    );
  }

  @Test
  void verifyCodeServerError() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.checkVerificationCodeSession(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new RuntimeException())));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.put(Entity.json(submitVerificationCodeJson("123456")))) {
      assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
    }
  }

  @Test
  void verifyCodeAlreadyVerified() {

    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        true, null, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.put(
        Entity.json(submitVerificationCodeJson("123456")))) {

      verify(registrationServiceClient).getSession(any(), any());
      verifyNoMoreInteractions(registrationServiceClient);

      assertEquals(HttpStatus.SC_CONFLICT, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);
      assertTrue(verificationSessionResponse.verified());

      verify(registrationRecoveryPasswordsManager).removeForNumber(registrationServiceSession.number());
    }
  }

  @Test
  void verifyCodeNoCodeRequested() {

    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, 0L, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    // There is no explicit indication in the exception that no code has been sent, but we treat all RegistrationServiceExceptions
    // in which the response has a session object as conflicted state
    when(registrationServiceClient.checkVerificationCodeSession(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(
            new RegistrationServiceException(new RegistrationServiceSession(SESSION_ID, NUMBER, false, 0L, null, null,
                SESSION_EXPIRATION_SECONDS)))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.put(Entity.json(submitVerificationCodeJson("123456")))) {
      assertEquals(HttpStatus.SC_CONFLICT, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertNotNull(verificationSessionResponse.nextSms());
      assertNull(verificationSessionResponse.nextVerificationAttempt());
    }
  }

  @Test
  void verifyCodeNoSession() {

    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.checkVerificationCodeSession(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(new RegistrationServiceException(null))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.put(Entity.json(submitVerificationCodeJson("123456")))) {
      assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
    }
  }

  @Test
  void verifyCodeRateLimitExceeded() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.checkVerificationCodeSession(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(
            new CompletionException(new VerificationSessionRateLimitExceededException(registrationServiceSession,
                Duration.ofMinutes(1), true))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.put(Entity.json(submitVerificationCodeJson("567890")))) {
      assertEquals(HttpStatus.SC_TOO_MANY_REQUESTS, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.allowedToRequestCode());
      assertTrue(verificationSessionResponse.requestedInformation().isEmpty());
    }
  }

  @Test
  void verifyCodeSuccess() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, 0L, SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    final RegistrationServiceSession verifiedSession = new RegistrationServiceSession(SESSION_ID, NUMBER, true, null,
        null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.checkVerificationCodeSession(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(verifiedSession));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.put(Entity.json(submitVerificationCodeJson("123456")))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse = response.readEntity(
          VerificationSessionResponse.class);

      assertTrue(verificationSessionResponse.verified());

      verify(registrationRecoveryPasswordsManager).removeForNumber(verifiedSession.number());
    }
  }

  /**
   * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.CreateVerificationSessionRequest}
   */
  private static String createSessionJson(final String number, final String pushToken,
      final String pushTokenType) {
    return String.format("""
        {
          "number": %s,
          "pushToken": %s,
          "pushTokenType": %s
        }
        """, quoteIfNotNull(number), quoteIfNotNull(pushToken), quoteIfNotNull(pushTokenType));
  }

  /**
   * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.UpdateVerificationSessionRequest}
   */
  private static String updateSessionJson(final String captcha, final String pushChallenge, final String pushToken,
      final String pushTokenType) {
    return String.format("""
            {
              "captcha": %s,
              "pushChallenge": %s,
              "pushToken": %s,
              "pushTokenType": %s
            }
            """, quoteIfNotNull(captcha), quoteIfNotNull(pushChallenge), quoteIfNotNull(pushToken),
        quoteIfNotNull(pushTokenType));
  }

  /**
   * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.VerificationCodeRequest}
   */
  private static String requestVerificationCodeJson(final String transport, final String client) {
    return String.format("""
             {
               "transport": "%s",
               "client": "%s"
             }
        """, transport, client);
  }

  /**
   * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.SubmitVerificationCodeRequest}
   */
  private static String submitVerificationCodeJson(final String code) {
    return String.format("""
        {
          "code": "%s"
        }
        """, code);
  }

  private static String quoteIfNotNull(final String s) {
    return s == null ? null : StringUtils.join(new String[]{"\"", "\""}, s);
  }

  /**
   * Request JSON that cannot be marshalled into
   * {@link org.whispersystems.textsecuregcm.entities.CreateVerificationSessionRequest}
   */
  private static String unprocessableCreateSessionJson(final String number, final String pushToken,
      final String pushTokenType) {
    return String.format("""
        {
          "number": %s,
          "pushToken": %s,
          "pushTokenType": %s
        }
        """, number, quoteIfNotNull(pushToken), quoteIfNotNull(pushTokenType));
  }

  private static String encodeSessionId(final byte[] sessionId) {
    return Base64.getUrlEncoder().encodeToString(sessionId);
  }

}
