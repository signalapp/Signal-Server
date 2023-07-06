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
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.ChallengeConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.limits.RateLimitChallengeManager;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.ChallengeTokenBlinder;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;

@ExtendWith(DropwizardExtensionsSupport.class)
class ChallengeControllerTest {
  private static final RateLimitChallengeManager rateLimitChallengeManager = mock(RateLimitChallengeManager.class);

  private static final Accounts accounts = mock(Accounts.class);

  private static final TestClock clock = TestClock.now();
  
  private static final ChallengeTokenBlinder tokenBlinder = new ChallengeTokenBlinder(
      new ChallengeConfiguration(new SecretBytes("super secret key".getBytes()), Duration.ofMinutes(10)),
      clock);

  private static final ChallengeController challengeController = new ChallengeController(accounts, tokenBlinder, rateLimitChallengeManager);

  private static final ResourceExtension EXTENSION = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          Set.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new RateLimitExceededExceptionMapper())
      .addResource(challengeController)
      .build();

  @AfterEach
  void teardown() {
    reset(accounts);
    reset(rateLimitChallengeManager);
    clock.unpin();
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
    doThrow(new RateLimitExceededException(retryAfter, true)).when(rateLimitChallengeManager)
        .answerPushChallenge(any(), any());

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(pushChallengeJson));

    assertEquals(413, response.getStatus());
    assertEquals(String.valueOf(retryAfter.toSeconds()), response.getHeaderString("Retry-After"));
  }

  @Test
  void testHandleRecaptcha() throws RateLimitExceededException, IOException {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(200, response.getStatus());

    verify(rateLimitChallengeManager).answerRecaptchaChallenge(eq(AuthHelper.VALID_ACCOUNT), eq("The value of the solved captcha token"), eq("10.0.0.1"), anyString());
  }

  @Test
  void testHandleInvalidCaptcha() throws RateLimitExceededException, IOException {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    when(rateLimitChallengeManager.answerRecaptchaChallenge(eq(AuthHelper.VALID_ACCOUNT), eq("The value of the solved captcha token"), eq("10.0.0.1"), anyString()))
        .thenReturn(false);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(428, response.getStatus());
  }

  @Test
  void testHandleRecaptchaRateLimited() throws RateLimitExceededException, IOException {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    final Duration retryAfter = Duration.ofMinutes(17);
    doThrow(new RateLimitExceededException(retryAfter, true)).when(rateLimitChallengeManager)
        .answerRecaptchaChallenge(any(), any(), any(), any());

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(413, response.getStatus());
    assertEquals(String.valueOf(retryAfter.toSeconds()), response.getHeaderString("Retry-After"));
  }

  @Test
  void testHandleRecaptchaNoForwardedFor() {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "A server-generated token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(400, response.getStatus());
    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleRecaptchaWithTokenAuth() throws Exception {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "%s",
          "captcha": "The value of the solved captcha token"
        }
    """.formatted(tokenBlinder.generateBlindedAccountToken(AuthHelper.VALID_UUID));

    when(accounts.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(200, response.getStatus());

    verify(rateLimitChallengeManager).answerRecaptchaChallenge(eq(AuthHelper.VALID_ACCOUNT), eq("The value of the solved captcha token"), eq("10.0.0.1"), anyString());
  }

  @Test
  void testHandleRecaptchaWithExpiredToken() throws Exception {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "%s",
          "captcha": "The value of the solved captcha token"
        }
    """.formatted(tokenBlinder.generateBlindedAccountToken(AuthHelper.VALID_UUID));

    clock.pin(clock.instant().plus(Duration.ofMinutes(20)));
    when(accounts.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(401, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleRecaptchaWithPostdatedToken() throws Exception {
    clock.pin(clock.instant());
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "%s",
          "captcha": "The value of the solved captcha token"
        }
    """.formatted(tokenBlinder.generateBlindedAccountToken(AuthHelper.VALID_UUID));

    clock.pin(clock.instant().minus(Duration.ofMinutes(1)));
    when(accounts.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(401, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleRecaptchaNoAuthNonBase64Token() throws Exception {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "This is not a valid auth token",
          "captcha": "The value of the solved captcha token"
        }
        """;

    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(401, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleRecaptchaNoAuthValidBase64Token() throws Exception {
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "Y2x1Y2sgY2x1Y2ssIGknbSBhIHBhcnJvdAo=",
          "captcha": "The value of the solved captcha token"
        }
        """;

    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(401, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleRecaptchaNoAuthTokenEncryptedWithWrongKey() throws Exception {
    final String badToken =
        new ChallengeTokenBlinder(
            new ChallengeConfiguration(new SecretBytes("oh no, wrong key".getBytes()), Duration.ofMinutes(10)),
            clock)
        .generateBlindedAccountToken(AuthHelper.VALID_UUID);

    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "%s",
          "captcha": "The value of the solved captcha token"
        }
        """.formatted(badToken);

    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(401, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
  }

  @Test
  void testHandleRecaptchaWithTokenForBadAccount() throws Exception {
    final UUID badUUID = UUID.randomUUID();
    final String recaptchaChallengeJson = """
        {
          "type": "recaptcha",
          "token": "%s",
          "captcha": "The value of the solved captcha token"
        }
    """.formatted(tokenBlinder.generateBlindedAccountToken(badUUID));

    when(accounts.getByAccountIdentifier(badUUID)).thenReturn(Optional.empty());
    when(rateLimitChallengeManager.answerRecaptchaChallenge(any(), any(), any(), any()))
        .thenReturn(true);

    final Response response = EXTENSION.target("/v1/challenge")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "10.0.0.1")
        .put(Entity.json(recaptchaChallengeJson));

    assertEquals(401, response.getStatus());

    verifyNoInteractions(rateLimitChallengeManager);
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
