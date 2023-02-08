/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.RegistrationLockError;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationSession;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class RegistrationControllerTest {

  private static final String NUMBER = "+18005551212";
  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = mock(
      RegistrationLockVerificationManager.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);

  private final RateLimiter registrationLimiter = mock(RateLimiter.class);
  private final RateLimiter pinLimiter = mock(RateLimiter.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new RegistrationController(accountsManager, registrationServiceClient, registrationLockVerificationManager,
              registrationRecoveryPasswordsManager, rateLimiters))
      .build();

  @BeforeEach
  void setUp() {
    when(rateLimiters.getRegistrationLimiter()).thenReturn(registrationLimiter);
    when(rateLimiters.getPinLimiter()).thenReturn(pinLimiter);
  }

  @Test
  public void testRegistrationRequest() throws Exception {
    assertFalse(new RegistrationRequest("", new byte[0], new AccountAttributes(), true).isValid());
    assertFalse(new RegistrationRequest("some", new byte[32], new AccountAttributes(), true).isValid());
    assertTrue(new RegistrationRequest("", new byte[32], new AccountAttributes(), true).isValid());
    assertTrue(new RegistrationRequest("some", new byte[0], new AccountAttributes(), true).isValid());
  }

  @Test
  void unprocessableRequestJson() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request();
    try (Response response = request.post(Entity.json(unprocessableJson()))) {
      assertEquals(400, response.getStatus());
    }
  }

  @Test
  void missingBasicAuthorization() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request();
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(400, response.getStatus());
    }
  }

  @Test
  void invalidBasicAuthorization() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, "Basic but-invalid");
    try (Response response = request.post(Entity.json(invalidRequestJson()))) {
      assertEquals(401, response.getStatus());
    }
  }

  @Test
  void invalidRequestBody() {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(invalidRequestJson()))) {
      assertEquals(422, response.getStatus());
    }
  }

  @Test
  void rateLimitedSession() throws Exception {
    final String sessionId = "sessionId";
    doThrow(RateLimitExceededException.class)
        .when(registrationLimiter).validate(encodeSessionId(sessionId));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJson(sessionId)))) {
      assertEquals(413, response.getStatus());
      // In the future, change to assertEquals(429, response.getStatus());
    }
  }

  @Test
  void registrationServiceTimeout() {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, response.getStatus());
    }
  }

  @Test
  void recoveryPasswordManagerVerificationFailureOrTimeout() {
    when(registrationRecoveryPasswordsManager.verify(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJsonRecoveryPassword(new byte[32])))) {
      assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void registrationServiceSessionCheck(@Nullable final RegistrationSession session, final int expectedStatus,
      final String message) {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.ofNullable(session)));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(expectedStatus, response.getStatus(), message);
    }
  }

  static Stream<Arguments> registrationServiceSessionCheck() {
    return Stream.of(
        Arguments.of(null, 401, "session not found"),
        Arguments.of(new RegistrationSession("+18005551234", false), 400, "session number mismatch"),
        Arguments.of(new RegistrationSession(NUMBER, false), 401, "session not verified")
    );
  }

  @Test
  void recoveryPasswordManagerVerificationTrue() throws InterruptedException {
    when(registrationRecoveryPasswordsManager.verify(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));
    when(accountsManager.create(any(), any(), any(), any(), any()))
        .thenReturn(mock(Account.class));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    final byte[] recoveryPassword = new byte[32];
    try (Response response = request.post(Entity.json(requestJsonRecoveryPassword(recoveryPassword)))) {
      assertEquals(200, response.getStatus());
      Mockito.verify(registrationRecoveryPasswordsManager).storeForCurrentNumber(eq(NUMBER), eq(recoveryPassword));
    }
  }

  @Test
  void recoveryPasswordManagerVerificationFalse() throws InterruptedException {
    when(registrationRecoveryPasswordsManager.verify(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(false));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJsonRecoveryPassword(new byte[32])))) {
      assertEquals(403, response.getStatus());
    }
  }

  @ParameterizedTest
  @EnumSource(RegistrationLockError.class)
  void registrationLock(final RegistrationLockError error) throws Exception {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(new RegistrationSession(NUMBER, true))));

    when(accountsManager.getByE164(any())).thenReturn(Optional.of(mock(Account.class)));

    final Exception e = switch (error) {
      case MISMATCH -> new WebApplicationException(error.getExpectedStatus());
      case RATE_LIMITED -> new RateLimitExceededException(null);
    };
    doThrow(e)
        .when(registrationLockVerificationManager).verifyRegistrationLock(any(), any());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(error.getExpectedStatus(), response.getStatus());
    }
  }

  @ParameterizedTest
  @CsvSource({
      "false, false, false, 200",
      "true, false, false, 200",
      "true, false, true, 200",
      "true, true, false, 409",
      "true, true, true, 200"
  })
  void deviceTransferAvailable(final boolean existingAccount, final boolean transferSupported,
      final boolean skipDeviceTransfer, final int expectedStatus) throws Exception {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(new RegistrationSession(NUMBER, true))));

    final Optional<Account> maybeAccount;
    if (existingAccount) {
      final Account account = mock(Account.class);
      when(account.isTransferSupported()).thenReturn(transferSupported);
      maybeAccount = Optional.of(account);
    } else {
      maybeAccount = Optional.empty();
    }
    when(accountsManager.getByE164(any())).thenReturn(maybeAccount);
    when(accountsManager.create(any(), any(), any(), any(), any())).thenReturn(mock(Account.class));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJson("sessionId", new byte[0], skipDeviceTransfer)))) {
      assertEquals(expectedStatus, response.getStatus());
    }
  }

  // this is functionally the same as deviceTransferAvailable(existingAccount=false)
  @Test
  void success() throws Exception {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(new RegistrationSession(NUMBER, true))));
    when(accountsManager.create(any(), any(), any(), any(), any()))
        .thenReturn(mock(Account.class));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, authorizationHeader(NUMBER));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(200, response.getStatus());
    }
  }

  /**
   * Valid request JSON with the give session ID and skipDeviceTransfer
   */
  private static String requestJson(final String sessionId, final byte[] recoveryPassword, final boolean skipDeviceTransfer) {
    final String rp = encodeRecoveryPassword(recoveryPassword);
    return String.format("""
        {
          "sessionId": "%s",
          "recoveryPassword": "%s",
          "accountAttributes": {
            "recoveryPassword": "%s"
          },
          "skipDeviceTransfer": %s
        }
        """, encodeSessionId(sessionId), rp, rp, skipDeviceTransfer);
  }

  /**
   * Valid request JSON with the given session ID
   */
  private static String requestJson(final String sessionId) {
    return requestJson(sessionId, new byte[0], false);
  }

  /**
   * Valid request JSON with the given Recovery Password
   */
  private static String requestJsonRecoveryPassword(final byte[] recoveryPassword) {
    return requestJson("", recoveryPassword, false);
  }

  /**
   * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.RegistrationRequest}, but that fails
   * validation
   */
  private static String invalidRequestJson() {
    return """
        {
          "sessionId": null,
          "accountAttributes": {},
          "skipDeviceTransfer": false
        }
        """;
  }

  /**
   * Request JSON that cannot be marshalled into {@link org.whispersystems.textsecuregcm.entities.RegistrationRequest}
   */
  private static String unprocessableJson() {
    return """
        {
          "sessionId": []
        }
        """;
  }

  private static String authorizationHeader(final String number) {
    return "Basic " + Base64.getEncoder().encodeToString(
        String.format("%s:password", number).getBytes(StandardCharsets.UTF_8));
  }

  private static String encodeSessionId(final String sessionId) {
    return Base64.getEncoder().encodeToString(sessionId.getBytes(StandardCharsets.UTF_8));
  }

  private static String encodeRecoveryPassword(final byte[] recoveryPassword) {
    return Base64.getEncoder().encodeToString(recoveryPassword);
  }
}
