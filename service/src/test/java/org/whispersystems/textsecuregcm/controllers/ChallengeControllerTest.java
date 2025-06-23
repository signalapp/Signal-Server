/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.spam.ChallengeConstraintChecker;
import org.whispersystems.textsecuregcm.spam.ChallengeConstraintChecker.ChallengeConstraints;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class ChallengeControllerTest {

  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final RateLimitChallengeManager rateLimitChallengeManager = mock(RateLimitChallengeManager.class);
  private static final ChallengeConstraintChecker challengeConstraintChecker = mock(ChallengeConstraintChecker.class);

  private static final ChallengeController challengeController =
      new ChallengeController(accountsManager, rateLimitChallengeManager, challengeConstraintChecker);

  private static final ResourceExtension EXTENSION = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new TestRemoteAddressFilterProvider("127.0.0.1"))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new RateLimitExceededExceptionMapper())
      .addResource(challengeController)
      .build();

  @BeforeEach
  void setup() {
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID_TWO)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT_TWO));

    when(challengeConstraintChecker.challengeConstraints(any(), any()))
        .thenReturn(new ChallengeConstraints(true, Optional.empty()));
  }

  @AfterEach
  void teardown() {
    reset(rateLimitChallengeManager, challengeConstraintChecker);
  }

  @Test
  void testHandlePushChallenge() throws RateLimitExceededException {
    final String pushChallengeJson = """
        {
          "type": "rateLimitPushChallenge",
          "challenge": "Hello I am a push challenge token"
        }
        """;

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(pushChallengeJson));

    assertEquals(200, response.getStatus());
    verify(rateLimitChallengeManager).answerPushChallenge(AuthHelper.VALID_ACCOUNT, "Hello I am a push challenge token");
  }

  @Test
  void testHandlePushChallengeRateLimited() throws RateLimitExceededException {
    final String pushChallengeJson = """
        {
          "type": "rateLimitPushChallenge",
          "challenge": "Hello I am a push challenge token"
        }
        """;

    final Duration retryAfter = Duration.ofMinutes(17);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimitChallengeManager)
        .answerPushChallenge(any(), any());

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(pushChallengeJson));

    assertEquals(429, response.getStatus());
    assertEquals(String.valueOf(retryAfter.toSeconds()), response.getHeaderString("Retry-After"));
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false } )
  void testHandleCaptcha(boolean hasThreshold) throws RateLimitExceededException, IOException {
    final String captchaChallengeJson = """
        {
          "type": "captcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    when(rateLimitChallengeManager.answerCaptchaChallenge(any(), any(), any(), any(), any()))
        .thenReturn(true);


    if (hasThreshold) {
      when(challengeConstraintChecker.challengeConstraints(any(), any()))
          .thenReturn(new ChallengeConstraints(true, Optional.of(0.5F)));
    }
    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(captchaChallengeJson));

    assertEquals(200, response.getStatus());

    verify(rateLimitChallengeManager).answerCaptchaChallenge(eq(AuthHelper.VALID_ACCOUNT),
        eq("The value of the solved captcha token"), eq("127.0.0.1"), anyString(),
        eq(hasThreshold ? Optional.of(0.5f) : Optional.empty()));
  }

  @Test
  void testHandleInvalidCaptcha() throws RateLimitExceededException, IOException {
    final String captchaChallengeJson = """
        {
          "type": "captcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;
    when(rateLimitChallengeManager.answerCaptchaChallenge(eq(AuthHelper.VALID_ACCOUNT),
        eq("The value of the solved captcha token"), eq("127.0.0.1"), anyString(), any()))
        .thenReturn(false);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(captchaChallengeJson));

    assertEquals(428, response.getStatus());
  }

  @Test
  void testHandleCaptchaRateLimited() throws RateLimitExceededException, IOException {
    final String captchaChallengeJson = """
        {
          "type": "captcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    final Duration retryAfter = Duration.ofMinutes(17);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimitChallengeManager)
        .answerCaptchaChallenge(any(), any(), any(), any(), any());

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(captchaChallengeJson));

    assertEquals(429, response.getStatus());
    assertEquals(String.valueOf(retryAfter.toSeconds()), response.getHeaderString("Retry-After"));
  }

  @Test
  void testHandleUnrecognizedAnswer() {
    final String unrecognizedJson = """
        {
          "type": "unrecognized"
        }
        """;

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(unrecognizedJson));

    assertEquals(400, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testRequestPushChallenge() throws NotPushRegisteredException {
    {
      final Response response = EXTENSION.target("/v1/challenge/push")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .post(Entity.text(""));

      assertEquals(200, response.getStatus());
    }

    {
      doThrow(NotPushRegisteredException.class).when(rateLimitChallengeManager).sendPushChallenge(AuthHelper.VALID_ACCOUNT_TWO);

      final Response response = EXTENSION.target("/v1/challenge/push")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_TWO, AuthHelper.VALID_PASSWORD_TWO))
          .post(Entity.text(""));

      assertEquals(404, response.getStatus());
    }
  }

  @Test
  void testRequestPushChallengeNotPermitted() {
    when(challengeConstraintChecker.challengeConstraints(any(), any()))
        .thenReturn(new ChallengeConstraints(false, Optional.empty()));

    final Response response = EXTENSION.target("/v1/challenge/push")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.text(""));
    assertEquals(429, response.getStatus());
    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testAnswerPushChallengeNotPermitted() {
    when(challengeConstraintChecker.challengeConstraints(any(), any()))
        .thenReturn(new ChallengeConstraints(false, Optional.empty()));

    final String pushChallengeJson = """
        {
          "type": "rateLimitPushChallenge",
          "challenge": "Hello I am a push challenge token"
        }
        """;

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(pushChallengeJson));

    assertEquals(429, response.getStatus());
    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testValidationError() {
    final String unrecognizedJson = """
        {
          "type": "rateLimitPushChallenge"
        }
        """;

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(unrecognizedJson));

    assertEquals(422, response.getStatus());
  }
}
