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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.RegistrationCaptchaManager;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicRegistrationConfiguration;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.entities.VerificationSessionResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.ObsoletePhoneNumberFormatExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RegistrationServiceSenderExceptionMapper;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.registration.RegistrationFraudException;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceException;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceSenderException;
import org.whispersystems.textsecuregcm.registration.TransportNotAllowedException;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import org.whispersystems.textsecuregcm.spam.RegistrationFraudChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.VerificationSessionManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class VerificationControllerTest {

  private static final long SESSION_EXPIRATION_SECONDS = Duration.ofMinutes(10).toSeconds();

  private static final byte[] SESSION_ID = "session".getBytes(StandardCharsets.UTF_8);
  private static final String NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final UUID PNI = UUID.randomUUID();
  private final RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private final VerificationSessionManager verificationSessionManager = mock(VerificationSessionManager.class);
  private final PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private final RegistrationCaptchaManager registrationCaptchaManager = mock(RegistrationCaptchaManager.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final Clock clock = Clock.systemUTC();

  private final RateLimiter captchaLimiter = mock(RateLimiter.class);
  private final RateLimiter pushChallengeLimiter = mock(RateLimiter.class);
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(
      DynamicConfigurationManager.class);
  private final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .addProvider(new ObsoletePhoneNumberFormatExceptionMapper())
      .addProvider(new RegistrationServiceSenderExceptionMapper())
      .addProvider(new TestRemoteAddressFilterProvider("127.0.0.1"))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new VerificationController(registrationServiceClient, verificationSessionManager, pushNotificationManager,
              registrationCaptchaManager, registrationRecoveryPasswordsManager, phoneNumberIdentifiers, rateLimiters, accountsManager,
              RegistrationFraudChecker.noop(), dynamicConfigurationManager, clock))
      .build();

  @BeforeEach
  void setUp() {
    when(rateLimiters.getVerificationCaptchaLimiter())
        .thenReturn(captchaLimiter);
    when(rateLimiters.getVerificationPushChallengeLimiter())
        .thenReturn(pushChallengeLimiter);
    when(accountsManager.getByE164(any()))
        .thenReturn(Optional.empty());
    when(dynamicConfiguration.getRegistrationConfiguration())
        .thenReturn(new DynamicRegistrationConfiguration(false));
    when(dynamicConfigurationManager.getConfiguration())
        .thenReturn(dynamicConfiguration);
    when(phoneNumberIdentifiers.getPhoneNumberIdentifier(NUMBER))
        .thenReturn(CompletableFuture.completedFuture(PNI));
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
    when(registrationServiceClient.createRegistrationSession(any(), anyString(), anyBoolean(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));

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
    when(registrationServiceClient.createRegistrationSession(any(), anyString(), anyBoolean(), any()))
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
  void createBeninSessionSuccess(final String requestedNumber, final String expectedNumber) {
    when(registrationServiceClient.createRegistrationSession(any(), anyString(), anyBoolean(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new RegistrationServiceSession(SESSION_ID, requestedNumber, false, null, null, null,
                    SESSION_EXPIRATION_SECONDS)));
    when(verificationSessionManager.insert(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(createSessionJson(requestedNumber, "token", "fcm")))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      final ArgumentCaptor<Phonenumber.PhoneNumber> phoneNumberArgumentCaptor = ArgumentCaptor.forClass(
          Phonenumber.PhoneNumber.class);
      verify(registrationServiceClient).createRegistrationSession(phoneNumberArgumentCaptor.capture(), anyString(), anyBoolean(), any());
      final Phonenumber.PhoneNumber phoneNumber = phoneNumberArgumentCaptor.getValue();

      assertEquals(expectedNumber, PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164));
    }
  }

  private static Stream<Arguments> createBeninSessionSuccess() {
    // libphonenumber 8.13.50 and on generate new-format numbers for Benin
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    return Stream.of(
        Arguments.of(newFormatBeninE164, newFormatBeninE164),
        Arguments.of(NUMBER, NUMBER)
    );
  }

  @Test
  void createBeninSessionFailure() {
    // libphonenumber 8.13.50 and on generate new-format numbers for Benin
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");

    when(registrationServiceClient.createRegistrationSession(any(), anyString(), anyBoolean(), any()))
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
    try (Response response = request.post(Entity.json(createSessionJson(oldFormatBeninE164, "token", "fcm")))) {
      assertEquals(499, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void createSessionSuccess(final String pushToken, final String pushTokenType,
      final List<VerificationSession.Information> expectedRequestedInformation) {
    when(registrationServiceClient.createRegistrationSession(any(), anyString(), anyBoolean(), any()))
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void createSessionReregistration(final boolean isReregistration) throws NumberParseException {
    when(registrationServiceClient.createRegistrationSession(any(), anyString(), anyBoolean(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new RegistrationServiceSession(SESSION_ID, NUMBER, false, null, null, null,
                    SESSION_EXPIRATION_SECONDS)));

    when(verificationSessionManager.insert(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    when(accountsManager.getByE164(NUMBER))
        .thenReturn(isReregistration ? Optional.of(mock(Account.class)) : Optional.empty());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");

    try (final Response response = request.post(Entity.json(createSessionJson(NUMBER, null, null)))) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      verify(registrationServiceClient).createRegistrationSession(
          eq(PhoneNumberUtil.getInstance().parse(NUMBER, null)),
          anyString(),
          eq(isReregistration),
          any()
      );
    }
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
                    null, null, false, clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, false,
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, false,
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
                Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
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
                Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any(), any(), any()))
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
                List.of(VerificationSession.Information.PUSH_CHALLENGE),
                null, null, false,
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
            Optional.of(new VerificationSession("challenge", List.of(), List.of(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

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

      verify(registrationRecoveryPasswordsManager).remove(PNI);
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
                Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
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
                Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any(), any(), any()))
        .thenReturn(Optional.of(AssessmentResult.alwaysValid()));

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
                Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any(), any(), any()))
        .thenReturn(Optional.of(AssessmentResult.alwaysValid()));

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
                Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
                registrationServiceSession.expiration()))));

    when(registrationCaptchaManager.assessCaptcha(any(), any(), any(), any()))
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
  void getSessionInvalidArgs() {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new StatusRuntimeException(Status.INVALID_ARGUMENT)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodeSessionId(SESSION_ID))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_BAD_REQUEST, response.getStatus());
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

      verify(registrationRecoveryPasswordsManager).remove(PNI);
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any(), any()))
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
            VerificationSession.Information.CAPTCHA), Collections.emptyList(), null, null, false, clock.millis(), clock.millis(),
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, false,
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any(), any()))
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
  void requestVerificationCodeTransportNotAllowed() {
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null,
        null, SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(
            new CompletionException(new TransportNotAllowedException(registrationServiceSession))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");

    try (final Response response = request.post(Entity.json(requestVerificationCodeJson("sms", "android")))) {
      assertEquals(418, response.getStatus());

      final VerificationSessionResponse verificationSessionResponse =
          response.readEntity(VerificationSessionResponse.class);

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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any(), any()))
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any(), any()))
        .thenReturn(
            CompletableFuture.failedFuture(new CompletionException(exception)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("voice", "ios")))) {
      assertEquals(RegistrationServiceSenderExceptionMapper.REMOTE_SERVICE_REJECTED_REQUEST_STATUS, response.getStatus());

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fraudError(boolean shadowFailure) {
    if (shadowFailure) {
      when(this.dynamicConfiguration.getRegistrationConfiguration())
          .thenReturn(new DynamicRegistrationConfiguration(true));
    }
    final String encodedSessionId = encodeSessionId(SESSION_ID);
    final RegistrationServiceSession registrationServiceSession = new RegistrationServiceSession(SESSION_ID, NUMBER,
        false, null, null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(registrationServiceSession)));
    when(verificationSessionManager.findForId(any()))
        .thenReturn(CompletableFuture.completedFuture(
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.sendVerificationCode(any(), any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new CompletionException(
            new RegistrationFraudException(RegistrationServiceSenderException.rejected(true)))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/verification/session/" + encodedSessionId + "/code")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1");
    try (Response response = request.post(Entity.json(requestVerificationCodeJson("voice", "ios")))) {
      if (shadowFailure) {
        assertEquals(200, response.getStatus());
      } else {
        assertEquals(RegistrationServiceSenderExceptionMapper.REMOTE_SERVICE_REJECTED_REQUEST_STATUS, response.getStatus());
        final Map<String, Object> responseMap = response.readEntity(Map.class);
        assertEquals("providerRejected", responseMap.get("reason"));
      }
    }
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
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

      verify(registrationRecoveryPasswordsManager).remove(PNI);
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    // There is no explicit indication in the exception that no code has been sent, but we treat all RegistrationServiceExceptions
    // in which the response has a session object as conflicted state
    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));
    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
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
            Optional.of(new VerificationSession(null, Collections.emptyList(), Collections.emptyList(), null, null, true,
                clock.millis(), clock.millis(), registrationServiceSession.expiration()))));

    final RegistrationServiceSession verifiedSession = new RegistrationServiceSession(SESSION_ID, NUMBER, true, null,
        null, 0L,
        SESSION_EXPIRATION_SECONDS);
    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
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

      verify(registrationRecoveryPasswordsManager).remove(PNI);
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
