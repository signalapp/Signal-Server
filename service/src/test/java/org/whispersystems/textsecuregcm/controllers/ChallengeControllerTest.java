/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import java.util.Set;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.mappers.RetryLaterExceptionMapper;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class ChallengeControllerTest {

  private static final RateLimitChallengeManager rateLimitChallengeManager = mock(RateLimitChallengeManager.class);

  private static final ChallengeController challengeController = new ChallengeController(rateLimitChallengeManager);

  private static final ResourceExtension EXTENSION = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(Set.of(Account.class, DisabledPermittedAccount.class)))
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new RetryLaterExceptionMapper())
      .addResource(challengeController)
      .build();

  @AfterEach
  void teardown() {
    reset(rateLimitChallengeManager);
  }

  @Test
  void testHandlePushChallenge() throws RateLimitExceededException {
    final String pushChallengeJson = "{\n"
        + "  \"type\": \"rateLimitPushChallenge\",\n"
        + "  \"challenge\": \"Hello I am a push challenge token\"\n"
        + "}";

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(pushChallengeJson));

    assertEquals(200, response.getStatus());
    verify(rateLimitChallengeManager).answerPushChallenge(AuthHelper.VALID_ACCOUNT, "Hello I am a push challenge token");
  }

  @Test
  void testHandlePushChallengeRateLimited() throws RateLimitExceededException {
    final String pushChallengeJson = "{\n"
        + "  \"type\": \"rateLimitPushChallenge\",\n"
        + "  \"challenge\": \"Hello I am a push challenge token\"\n"
        + "}";

    final Duration retryAfter = Duration.ofMinutes(17);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimitChallengeManager).answerPushChallenge(any(), any());

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(pushChallengeJson));

    assertEquals(413, response.getStatus());
    assertEquals(String.valueOf(retryAfter.toSeconds()), response.getHeaderString("Retry-After"));
  }

  @Test
  void testHandleRecaptcha() throws RateLimitExceededException {
    final String recaptchaChallengeJson = "{\n"
        + "  \"type\": \"recaptcha\",\n"
        + "  \"token\": \"A server-generated token\",\n"
        + "  \"captcha\": \"The value of the solved captcha token\"\n"
        + "}";

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("X-Forwarded-For", "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(200, response.getStatus());
    verify(rateLimitChallengeManager).answerRecaptchaChallenge(AuthHelper.VALID_ACCOUNT, "The value of the solved captcha token", "10.0.0.1");
  }

  @Test
  void testHandleRecaptchaRateLimited() throws RateLimitExceededException {
    final String recaptchaChallengeJson = "{\n"
        + "  \"type\": \"recaptcha\",\n"
        + "  \"token\": \"A server-generated token\",\n"
        + "  \"captcha\": \"The value of the solved captcha token\"\n"
        + "}";

    final Duration retryAfter = Duration.ofMinutes(17);
    doThrow(new RateLimitExceededException(retryAfter)).when(rateLimitChallengeManager).answerRecaptchaChallenge(any(), any(), any());

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("X-Forwarded-For", "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(413, response.getStatus());
    assertEquals(String.valueOf(retryAfter.toSeconds()), response.getHeaderString("Retry-After"));
  }

  @Test
  void testHandleRecaptchaNoForwardedFor() {
    final String recaptchaChallengeJson = "{\n"
        + "  \"type\": \"recaptcha\",\n"
        + "  \"token\": \"A server-generated token\",\n"
        + "  \"captcha\": \"The value of the solved captcha token\"\n"
        + "}";

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(400, response.getStatus());
    verifyZeroInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleUnrecognizedAnswer() {
    final String unrecognizedJson = "{\n"
        + "  \"type\": \"unrecognized\"\n"
        + "}";

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("X-Forwarded-For", "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(unrecognizedJson));

    assertEquals(400, response.getStatus());

    verifyZeroInteractions(rateLimitChallengeManager);
  }

  @Test
  void testRequestPushChallenge() throws NotPushRegisteredException {
    {
      final Response response = EXTENSION.target("/v1/challenge/push")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
          .post(Entity.text(""));

      assertEquals(200, response.getStatus());
    }

    {
      doThrow(NotPushRegisteredException.class).when(rateLimitChallengeManager).sendPushChallenge(AuthHelper.VALID_ACCOUNT_TWO);

      final Response response = EXTENSION.target("/v1/challenge/push")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER_TWO, AuthHelper.VALID_PASSWORD_TWO))
          .post(Entity.text(""));

      assertEquals(404, response.getStatus());
    }
  }

  @Test
  void testValidationError() {
    final String unrecognizedJson = "{\n"
        + "  \"type\": \"rateLimitPushChallenge\"\n"
        + "}";

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(unrecognizedJson));

    assertEquals(422, response.getStatus());
  }
}
