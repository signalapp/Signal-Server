/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockError;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.PhoneNumberDiscoverabilityRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationSession;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class AccountControllerV2Test {

  public static final String NEW_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final ChangeNumberManager changeNumberManager = mock(ChangeNumberManager.class);
  private final RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = mock(
      RegistrationLockVerificationManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final RateLimiter registrationLimiter = mock(RateLimiter.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(
          new PolymorphicAuthValueFactoryProvider.Binder<>(
              ImmutableSet.of(AuthenticatedAccount.class,
                  DisabledPermittedAuthenticatedAccount.class)))
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new AccountControllerV2(accountsManager, changeNumberManager,
              new PhoneVerificationTokenManager(registrationServiceClient, registrationRecoveryPasswordsManager),
              registrationLockVerificationManager, rateLimiters))
      .build();

  @Nested
  class ChangeNumber {

    @BeforeEach
    void setUp() throws Exception {
      when(rateLimiters.getRegistrationLimiter()).thenReturn(registrationLimiter);

      when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any())).thenAnswer(
          (Answer<Account>) invocation -> {
            final Account account = invocation.getArgument(0, Account.class);
            final String number = invocation.getArgument(1, String.class);
            final String pniIdentityKey = invocation.getArgument(2, String.class);

            final UUID uuid = account.getUuid();
            final List<Device> devices = account.getDevices();

            final Account updatedAccount = mock(Account.class);
            when(updatedAccount.getUuid()).thenReturn(uuid);
            when(updatedAccount.getNumber()).thenReturn(number);
            when(updatedAccount.getPhoneNumberIdentityKey()).thenReturn(pniIdentityKey);
            when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(UUID.randomUUID());
            when(updatedAccount.getDevices()).thenReturn(devices);

            for (long i = 1; i <= 3; i++) {
              final Optional<Device> d = account.getDevice(i);
              when(updatedAccount.getDevice(i)).thenReturn(d);
            }

            return updatedAccount;
          });
    }

    @Test
    void changeNumberSuccess() throws Exception {

      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(new RegistrationSession(NEW_NUMBER, true))));

      final AccountIdentityResponse accountIdentityResponse =
          resources.getJerseyTest()
              .target("/v2/accounts/number")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.entity(
                  new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", "123",
                      Collections.emptyList(),
                      Collections.emptyMap(), Collections.emptyMap()),
                  MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

      verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(NEW_NUMBER), any(), any(), any(),
          any());

      assertEquals(AuthHelper.VALID_UUID, accountIdentityResponse.uuid());
      assertEquals(NEW_NUMBER, accountIdentityResponse.number());
      assertNotEquals(AuthHelper.VALID_PNI, accountIdentityResponse.pni());
    }

    @Test
    void unprocessableRequestJson() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(unprocessableJson()))) {
        assertEquals(400, response.getStatus());
      }
    }

    @Test
    void missingBasicAuthorization() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request();
      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(401, response.getStatus());
      }
    }

    @Test
    void invalidBasicAuthorization() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION, "Basic but-invalid");
      try (Response response = request.put(Entity.json(invalidRequestJson()))) {
        assertEquals(401, response.getStatus());
      }
    }

    @Test
    void invalidRequestBody() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(invalidRequestJson()))) {
        assertEquals(422, response.getStatus());
      }
    }

    @Test
    void rateLimitedNumber() throws Exception {
      doThrow(new RateLimitExceededException(null, true))
          .when(registrationLimiter).validate(anyString());

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(429, response.getStatus());
      }
    }

    @Test
    void registrationServiceTimeout() {
      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
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
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(expectedStatus, response.getStatus(), message);
      }
    }

    static Stream<Arguments> registrationServiceSessionCheck() {
      return Stream.of(
          Arguments.of(null, 401, "session not found"),
          Arguments.of(new RegistrationSession("+18005551234", false), 400, "session number mismatch"),
          Arguments.of(new RegistrationSession(NEW_NUMBER, false), 401, "session not verified")
      );
    }

    @ParameterizedTest
    @EnumSource(RegistrationLockError.class)
    void registrationLock(final RegistrationLockError error) throws Exception {
      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(
              CompletableFuture.completedFuture(Optional.of(new RegistrationSession(NEW_NUMBER, true))));

      when(accountsManager.getByE164(any())).thenReturn(Optional.of(mock(Account.class)));

      final Exception e = switch (error) {
        case MISMATCH -> new WebApplicationException(error.getExpectedStatus());
        case RATE_LIMITED -> new RateLimitExceededException(null, true);
      };
      doThrow(e)
          .when(registrationLockVerificationManager).verifyRegistrationLock(any(), any());

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(requestJson("sessionId", NEW_NUMBER)))) {
        assertEquals(error.getExpectedStatus(), response.getStatus());
      }
    }

    @Test
    void recoveryPasswordManagerVerificationTrue() throws Exception {
      when(registrationRecoveryPasswordsManager.verify(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(true));

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      final byte[] recoveryPassword = new byte[32];
      try (Response response = request.put(Entity.json(requestJsonRecoveryPassword(recoveryPassword, NEW_NUMBER)))) {
        assertEquals(200, response.getStatus());

        final AccountIdentityResponse accountIdentityResponse = response.readEntity(AccountIdentityResponse.class);

        verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(NEW_NUMBER), any(), any(), any(),
            any());

        assertEquals(AuthHelper.VALID_UUID, accountIdentityResponse.uuid());
        assertEquals(NEW_NUMBER, accountIdentityResponse.number());
        assertNotEquals(AuthHelper.VALID_PNI, accountIdentityResponse.pni());
      }
    }

    @Test
    void recoveryPasswordManagerVerificationFalse() {
      when(registrationRecoveryPasswordsManager.verify(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(false));

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(requestJsonRecoveryPassword(new byte[32], NEW_NUMBER)))) {
        assertEquals(403, response.getStatus());
      }
    }

    /**
     * Valid request JSON with the given Recovery Password
     */
    private static String requestJsonRecoveryPassword(final byte[] recoveryPassword, final String newNumber) {
      return requestJson("", recoveryPassword, newNumber);
    }

    /**
     * Valid request JSON with the give session ID and recovery password
     */
    private static String requestJson(final String sessionId, final byte[] recoveryPassword, final String newNumber) {
      return String.format("""
          {
            "sessionId": "%s",
            "recoveryPassword": "%s",
            "number": "%s",
            "reglock": "1234",
            "pniIdentityKey": "5678",
            "deviceMessages": [],
            "devicePniSignedPrekeys": {},
            "pniRegistrationIds": {}
          }
          """, encodeSessionId(sessionId), encodeRecoveryPassword(recoveryPassword), newNumber);
    }

    /**
     * Valid request JSON with the give session ID
     */
    private static String requestJson(final String sessionId, final String newNumber) {
      return requestJson(sessionId, new byte[0], newNumber);
    }

    /**
     * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.ChangeNumberRequest}, but that
     * fails validation
     */
    private static String invalidRequestJson() {
      return """
          {
            "sessionId": null
          }
          """;
    }

    /**
     * Request JSON that cannot be marshalled into
     * {@link org.whispersystems.textsecuregcm.entities.ChangeNumberRequest}
     */
    private static String unprocessableJson() {
      return """
          {
            "sessionId": []
          }
          """;
    }

    private static String encodeSessionId(final String sessionId) {
      return Base64.getUrlEncoder().encodeToString(sessionId.getBytes(StandardCharsets.UTF_8));
    }

    private static String encodeRecoveryPassword(final byte[] recoveryPassword) {
      return Base64.getEncoder().encodeToString(recoveryPassword);
    }
  }

  @Nested
  class PhoneNumberDiscoverability {

    @BeforeEach
    void setup() {
      AccountsHelper.setupMockUpdate(accountsManager);
    }
    @Test
    void testSetPhoneNumberDiscoverability() {
      Response response = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_discoverability")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.json(new PhoneNumberDiscoverabilityRequest(true)));

      assertThat(response.getStatus()).isEqualTo(204);

      ArgumentCaptor<Boolean> discoverabilityCapture = ArgumentCaptor.forClass(Boolean.class);
      verify(AuthHelper.VALID_ACCOUNT).setDiscoverableByPhoneNumber(discoverabilityCapture.capture());
      assertThat(discoverabilityCapture.getValue()).isTrue();
    }

    @Test
    void testSetNullPhoneNumberDiscoverability() {
      Response response = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_discoverability")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.json(
            """
            {
              "discoverableByPhoneNumber": null
            }
            """));

      assertThat(response.getStatus()).isEqualTo(422);
      verify(AuthHelper.VALID_ACCOUNT, never()).setDiscoverableByPhoneNumber(anyBoolean());
    }
  }
}
