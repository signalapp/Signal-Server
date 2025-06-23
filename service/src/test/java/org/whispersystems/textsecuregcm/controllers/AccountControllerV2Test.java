/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockError;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.entities.AccountDataReportResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.PhoneNumberDiscoverabilityRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.spam.RegistrationRecoveryChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

@ExtendWith(DropwizardExtensionsSupport.class)
class AccountControllerV2Test {

  private static final long SESSION_EXPIRATION_SECONDS = Duration.ofMinutes(10).toSeconds();

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();
  private static final IdentityKey IDENTITY_KEY = new IdentityKey(IDENTITY_KEY_PAIR.getPublicKey());

  private static final String NEW_NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final ChangeNumberManager changeNumberManager = mock(ChangeNumberManager.class);
  private final PhoneNumberIdentifiers phoneNumberIdentifiers = mock(PhoneNumberIdentifiers.class);
  private final RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = mock(
      RegistrationLockVerificationManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final RateLimiter registrationLimiter = mock(RateLimiter.class);
  private final RegistrationRecoveryChecker registrationRecoveryChecker = mock(RegistrationRecoveryChecker.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new AccountControllerV2(accountsManager, changeNumberManager,
              new PhoneVerificationTokenManager(phoneNumberIdentifiers, registrationServiceClient,
                  registrationRecoveryPasswordsManager, registrationRecoveryChecker),
              registrationLockVerificationManager, rateLimiters))
      .build();

  @Nested
  class ChangeNumber {

    @BeforeEach
    void setUp() throws Exception {
      when(rateLimiters.getRegistrationLimiter()).thenReturn(registrationLimiter);

      when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

      when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any())).thenAnswer(
          (Answer<Account>) invocation -> {
            final Account account = invocation.getArgument(0);
            final String number = invocation.getArgument(1);
            final IdentityKey pniIdentityKey = invocation.getArgument(2);

            final UUID uuid = account.getUuid();
            final List<Device> devices = account.getDevices();

            final Account updatedAccount = mock(Account.class);
            when(updatedAccount.getUuid()).thenReturn(uuid);
            when(updatedAccount.getNumber()).thenReturn(number);
            when(updatedAccount.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);
            if (number.equals(account.getNumber())) {
              when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(AuthHelper.VALID_PNI);
            } else {
              when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(UUID.randomUUID());
            }
            when(updatedAccount.getDevices()).thenReturn(devices);

            for (byte i = 1; i <= 3; i++) {
              final Optional<Device> d = account.getDevice(i);
              when(updatedAccount.getDevice(i)).thenReturn(d);
            }

            return updatedAccount;
          });
    }

    @Test
    void changeNumberSuccess() throws Exception {

      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(
              Optional.of(new RegistrationServiceSession(new byte[16], NEW_NUMBER, true, null, null, null,
                  SESSION_EXPIRATION_SECONDS))));

      final AccountIdentityResponse accountIdentityResponse =
          resources.getJerseyTest()
              .target("/v2/accounts/number")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.entity(
                  new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", IDENTITY_KEY,
                      Collections.emptyList(),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, 17)),
                  MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

      verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(NEW_NUMBER), any(), any(), any(),
          any(), any(), any());

      assertEquals(AuthHelper.VALID_UUID, accountIdentityResponse.uuid());
      assertEquals(NEW_NUMBER, accountIdentityResponse.number());
      assertNotEquals(AuthHelper.VALID_PNI, accountIdentityResponse.pni());
    }

    @Test
    void changeNumberSameNumber() throws Exception {
      final AccountIdentityResponse accountIdentityResponse =
          resources.getJerseyTest()
              .target("/v2/accounts/number")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.entity(
                  new ChangeNumberRequest(encodeSessionId("session"), null, AuthHelper.VALID_NUMBER, null, IDENTITY_KEY,
                      Collections.emptyList(),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, 17)),
                  MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

      verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(AuthHelper.VALID_NUMBER), any(), any(), any(),
          any(), any(), any());

      assertEquals(AuthHelper.VALID_UUID, accountIdentityResponse.uuid());
      assertEquals(AuthHelper.VALID_NUMBER, accountIdentityResponse.number());
      assertEquals(AuthHelper.VALID_PNI, accountIdentityResponse.pni());
    }

    @Test
    void changeNumberNonNormalizedNumber() {
      try (Response response = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.entity(
              // +4407700900111 is a valid number but not normalized - it has an optional '0' after the country code
              new ChangeNumberRequest(encodeSessionId("session"), null, "+4407700900111", null,
                  new IdentityKey(Curve.generateKeyPair().getPublicKey()),
                  Collections.emptyList(),
                  Collections.emptyMap(), null, Collections.emptyMap()),
              MediaType.APPLICATION_JSON_TYPE))) {
        assertEquals(422, response.getStatus());
      }
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

    @ParameterizedTest
    @MethodSource
    void invalidRegistrationId(final Integer pniRegistrationId, final int expectedStatusCode) {
      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(
              Optional.of(new RegistrationServiceSession(new byte[16], NEW_NUMBER, true, null, null, null,
                  SESSION_EXPIRATION_SECONDS))));
      final ChangeNumberRequest changeNumberRequest = new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", IDENTITY_KEY,
          Collections.emptyList(),
          Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
          Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
          Map.of(Device.PRIMARY_ID, pniRegistrationId));

      try (final Response response = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.entity(changeNumberRequest, MediaType.APPLICATION_JSON_TYPE))) {

        assertEquals(expectedStatusCode, response.getStatus());
      }
    }

    private static Stream<Arguments> invalidRegistrationId() {
      return Stream.of(
          Arguments.of(0x3FFF, 200),
          Arguments.of(0, 422),
          Arguments.of(-1, 422),
          Arguments.of(0x3FFF + 1, 422),
          Arguments.of(Integer.MAX_VALUE, 422)
      );
    }

    @Test
    void rateLimitedNumber() throws Exception {
      doThrow(new RateLimitExceededException(null))
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
    void registrationServiceSessionCheck(@Nullable final RegistrationServiceSession session, final int expectedStatus,
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
          Arguments.of(new RegistrationServiceSession(new byte[16], "+18005551234", false, null, null, null,
                  SESSION_EXPIRATION_SECONDS), 400,
              "session number mismatch"),
          Arguments.of(
              new RegistrationServiceSession(new byte[16], NEW_NUMBER, false, null, null, null,
                  SESSION_EXPIRATION_SECONDS),
              401,
              "session not verified")
      );
    }

    @ParameterizedTest
    @EnumSource(RegistrationLockError.class)
    void registrationLock(final RegistrationLockError error) throws Exception {
      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(
              CompletableFuture.completedFuture(
                  Optional.of(new RegistrationServiceSession(new byte[16], NEW_NUMBER, true, null, null, null,
                      SESSION_EXPIRATION_SECONDS))));

      when(accountsManager.getByE164(any())).thenReturn(Optional.of(mock(Account.class)));

      final Exception e = switch (error) {
        case MISMATCH -> new WebApplicationException(error.getExpectedStatus());
        case RATE_LIMITED -> new RateLimitExceededException(null);
      };
      doThrow(e)
          .when(registrationLockVerificationManager).verifyRegistrationLock(any(), any(), any(), any(), any());

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
      when(phoneNumberIdentifiers.getPhoneNumberIdentifier(any()))
          .thenReturn(CompletableFuture.completedFuture(UUID.randomUUID()));
      when(registrationRecoveryPasswordsManager.verify(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(true));
      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(any(), any()))
          .thenReturn(true);

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
            any(), any(), any());

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

    @Test
    void registrationRecoveryCheckerAllowsAttempt() {
      when(phoneNumberIdentifiers.getPhoneNumberIdentifier(any()))
          .thenReturn(CompletableFuture.completedFuture(UUID.randomUUID()));
      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(any(), any())).thenReturn(true);
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
      }
    }

    @Test
    void registrationRecoveryCheckerDisallowsAttempt() {
      when(registrationRecoveryChecker.checkRegistrationRecoveryAttempt(any(), any())).thenReturn(false);
      when(registrationRecoveryPasswordsManager.verify(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(true));

      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/number")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      final byte[] recoveryPassword = new byte[32];
      try (Response response = request.put(Entity.json(requestJsonRecoveryPassword(recoveryPassword, NEW_NUMBER)))) {
        assertEquals(403, response.getStatus());
      }
    }

    @Test
    void deviceMessageTooLarge() throws Exception {

      when(registrationServiceClient.getSession(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(
              Optional.of(new RegistrationServiceSession(new byte[16], NEW_NUMBER, true, null, null, null,
                  SESSION_EXPIRATION_SECONDS))));

      reset(changeNumberManager);
      when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any()))
          .thenThrow(MessageTooLargeException.class);

      try (final Response response = resources.getJerseyTest()
              .target("/v2/accounts/number")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.entity(
                  new ChangeNumberRequest(encodeSessionId("session"), null, NEW_NUMBER, "123", IDENTITY_KEY,
                      Collections.emptyList(),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR)),
                      Map.of(Device.PRIMARY_ID, 17)),
                  MediaType.APPLICATION_JSON_TYPE))) {

        assertEquals(413, response.getStatus());
      }
    }

    /**
     * Valid request JSON with the given Recovery Password
     */
    private static String requestJsonRecoveryPassword(final byte[] recoveryPassword, final String newNumber) {
      return requestJson("", recoveryPassword, newNumber, 123);
    }

    /**
     * Valid request JSON with the give session ID and recovery password
     */
    private static String requestJson(final String sessionId,
        final byte[] recoveryPassword,
        final String newNumber,
        final Integer pniRegistrationId) {

      final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR);
      final KEMSignedPreKey pniLastResortPreKey = KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR);

      return String.format("""
          {
            "sessionId": "%s",
            "recoveryPassword": "%s",
            "number": "%s",
            "reglock": "1234",
            "pniIdentityKey": "%s",
            "deviceMessages": [],
            "devicePniSignedPrekeys": {"1": {"keyId": %d, "publicKey": "%s", "signature": "%s"}},
            "devicePniPqLastResortPrekeys": {"1": {"keyId": %d, "publicKey": "%s", "signature": "%s"}},
            "pniRegistrationIds": {"1": %d}
          }
          """, encodeSessionId(sessionId),
          encodeRecoveryPassword(recoveryPassword),
          newNumber,
          Base64.getEncoder().encodeToString(IDENTITY_KEY.serialize()),
          pniSignedPreKey.keyId(), Base64.getEncoder().encodeToString(pniSignedPreKey.serializedPublicKey()), Base64.getEncoder().encodeToString(pniSignedPreKey.signature()),
          pniLastResortPreKey.keyId(), Base64.getEncoder().encodeToString(pniLastResortPreKey.serializedPublicKey()), Base64.getEncoder().encodeToString(pniLastResortPreKey.signature()),
          pniRegistrationId);
    }

    /**
     * Valid request JSON with the give session ID
     */
    private static String requestJson(final String sessionId, final String newNumber) {
      return requestJson(sessionId, new byte[0], newNumber, 123);
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
  class PhoneNumberIdentityKeyDistribution {

    @BeforeEach
    void setUp() throws Exception {
      when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

      when(changeNumberManager.updatePniKeys(any(), any(), any(), any(), any(), any(), any())).thenAnswer(
          (Answer<Account>) invocation -> {
            final Account account = invocation.getArgument(0);
            final IdentityKey pniIdentityKey = invocation.getArgument(1);

            final UUID uuid = account.getUuid();
            final UUID pni = account.getPhoneNumberIdentifier();
            final String number = account.getNumber();
            final List<Device> devices = account.getDevices();

            final Account updatedAccount = mock(Account.class);
            when(updatedAccount.getUuid()).thenReturn(uuid);
            when(updatedAccount.getNumber()).thenReturn(number);
            when(updatedAccount.getIdentityKey(IdentityType.PNI)).thenReturn(pniIdentityKey);
            when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(pni);
            when(updatedAccount.getDevices()).thenReturn(devices);

            for (byte i = 1; i <= 3; i++) {
              final Optional<Device> d = account.getDevice(i);
              when(updatedAccount.getDevice(i)).thenReturn(d);
            }

            return updatedAccount;
          });
    }

    @Test
    void pniKeyDistributionSuccess() throws Exception {
      final AccountIdentityResponse accountIdentityResponse =
          resources.getJerseyTest()
          .target("/v2/accounts/phone_number_identity_key_distribution")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
          .put(Entity.json(requestJson()), AccountIdentityResponse.class);

      verify(changeNumberManager).updatePniKeys(eq(AuthHelper.VALID_ACCOUNT), eq(IDENTITY_KEY), any(), any(), any(), any(), any());

      assertEquals(AuthHelper.VALID_UUID, accountIdentityResponse.uuid());
      assertEquals(AuthHelper.VALID_NUMBER, accountIdentityResponse.number());
      assertEquals(AuthHelper.VALID_PNI, accountIdentityResponse.pni());
    }

    @Test
    void unprocessableRequestJson() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_identity_key_distribution")
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
          .target("/v2/accounts/phone_number_identity_key_distribution")
          .request();
      try (Response response = request.put(Entity.json(requestJson()))) {
        assertEquals(401, response.getStatus());
      }
    }

    @Test
    void invalidBasicAuthorization() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_identity_key_distribution")
          .request()
          .header(HttpHeaders.AUTHORIZATION, "Basic but-invalid");
      try (Response response = request.put(Entity.json(requestJson()))) {
        assertEquals(401, response.getStatus());
      }
    }

    @Test
    void invalidRequestBody() {
      final Invocation.Builder request = resources.getJerseyTest()
          .target("/v2/accounts/phone_number_identity_key_distribution")
          .request()
          .header(HttpHeaders.AUTHORIZATION,
              AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
      try (Response response = request.put(Entity.json(invalidRequestJson()))) {
        assertEquals(422, response.getStatus());
      }
    }

    @Test
    void deviceMessageTooLarge() throws Exception {
      reset(changeNumberManager);
      when(changeNumberManager.updatePniKeys(any(), any(), any(), any(), any(), any(), any()))
          .thenThrow(MessageTooLargeException.class);

      try (final Response response = resources.getJerseyTest()
              .target("/v2/accounts/phone_number_identity_key_distribution")
              .request()
              .header(HttpHeaders.AUTHORIZATION,
                  AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
              .put(Entity.json(requestJson()))) {

        assertEquals(413, response.getStatus());
      }
    }

    /**
     * Valid request JSON for a {@link org.whispersystems.textsecuregcm.entities.PhoneNumberIdentityKeyDistributionRequest}
     */
    private static String requestJson() {
      final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(1, IDENTITY_KEY_PAIR);
      final KEMSignedPreKey pniLastResortPreKey = KeysHelper.signedKEMPreKey(2, IDENTITY_KEY_PAIR);

      return String.format("""
          {
            "pniIdentityKey": "%s",
            "deviceMessages": [],
            "devicePniSignedPrekeys": {},
            "devicePniSignedPrekeys": {"1": {"keyId": %d, "publicKey": "%s", "signature": "%s"}},
            "devicePniPqLastResortPrekeys": {"1": {"keyId": %d, "publicKey": "%s", "signature": "%s"}},
            "pniRegistrationIds": {"1": 17}
          }
      """, Base64.getEncoder().encodeToString(IDENTITY_KEY.serialize()),
          pniSignedPreKey.keyId(), Base64.getEncoder().encodeToString(pniSignedPreKey.serializedPublicKey()), Base64.getEncoder().encodeToString(pniSignedPreKey.signature()),
          pniLastResortPreKey.keyId(), Base64.getEncoder().encodeToString(pniLastResortPreKey.serializedPublicKey()), Base64.getEncoder().encodeToString(pniLastResortPreKey.signature()));
    }

    /**
     * Request JSON in the shape of {@link org.whispersystems.textsecuregcm.entities.PhoneNumberIdentityKeyDistributionRequest}, but that
     * fails validation
     */
    private static String invalidRequestJson() {
      return """
          {
            "pniIdentityKey": null,
            "deviceMessages": [],
            "devicePniSignedPrekeys": {},
            "pniRegistrationIds": {}
          }
          """;
    }

    /**
     * Request JSON that cannot be marshalled into
     * {@link org.whispersystems.textsecuregcm.entities.PhoneNumberIdentityKeyDistributionRequest}
     */
    private static String unprocessableJson() {
      return """
          {
            "pniIdentityKey": []
          }
          """;
    }

  }

  @Nested
  class PhoneNumberDiscoverability {

    @BeforeEach
    void setup() {
      AccountsHelper.setupMockUpdate(accountsManager);
      when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
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

    @ParameterizedTest
    @MethodSource
    void testGetAccountDataReport(final Account account, final String expectedTextAfterHeader) throws Exception {
      when(AuthHelper.ACCOUNTS_MANAGER.getByAccountIdentifier(account.getUuid())).thenReturn(Optional.of(account));
      when(accountsManager.getByAccountIdentifier(account.getUuid())).thenReturn(Optional.of(account));

      final Response response = resources.getJerseyTest()
          .target("/v2/accounts/data_report")
          .request()
          .header("Authorization", AuthHelper.getAuthHeader(account.getUuid(), "password"))
          .get();

      assertEquals(200, response.getStatus());

      final String stringResponse = response.readEntity(String.class);

      final AccountDataReportResponse structuredResponse = SystemMapper.jsonMapper()
          .readValue(stringResponse, AccountDataReportResponse.class);

      assertEquals(account.getNumber(), structuredResponse.data().account().phoneNumber());
      assertEquals(account.isDiscoverableByPhoneNumber(),
          structuredResponse.data().account().findAccountByPhoneNumber());
      assertEquals(account.isUnrestrictedUnidentifiedAccess(),
          structuredResponse.data().account().allowSealedSenderFromAnyone());

      final Set<Byte> deviceIds = account.getDevices().stream().map(Device::getId).collect(Collectors.toSet());

      // all devices should be present
      structuredResponse.data().devices().forEach(deviceDataReport -> {
        assertTrue(deviceIds.remove(deviceDataReport.id()));
        assertEquals(account.getDevice(deviceDataReport.id()).orElseThrow().getUserAgent(),
            deviceDataReport.userAgent());
      });
      assertTrue(deviceIds.isEmpty());

      final String actualText = (String) SystemMapper.jsonMapper().readValue(stringResponse, Map.class).get("text");
      final int headerEnd = actualText.indexOf("# Account");
      assertEquals(expectedTextAfterHeader, actualText.substring(headerEnd));

      final String actualHeader = actualText.substring(0, headerEnd);
      assertTrue(actualHeader.matches(
          "Report ID: [a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}\nReport timestamp: \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z\n\n"));
    }

    static Stream<Arguments> testGetAccountDataReport() {
      final String exampleNumber1 = toE164(PhoneNumberUtil.getInstance().getExampleNumber("ES"));
      final String account2PhoneNumber = toE164(PhoneNumberUtil.getInstance().getExampleNumber("AU"));
      final String account3PhoneNumber = toE164(PhoneNumberUtil.getInstance().getExampleNumber("IN"));

      final Instant account1Device1Created = Instant.ofEpochSecond(1669323142); // 2022-11-24T20:52:22Z
      final Instant account1Device2Created = Instant.ofEpochSecond(1679155122); // 2023-03-18T15:58:42Z
      final Instant account1Device1LastSeen = Instant.ofEpochMilli(Util.todayInMillis());
      final Instant account1Device2LastSeen = Instant.ofEpochSecond(1678838400); // 2023-03-15T00:00:00Z

      final Instant account2Device1Created = Instant.ofEpochSecond(1659123001); // 2022-07-29T19:30:01Z
      final Instant account2Device1LastSeen = Instant.ofEpochMilli(Util.todayInMillis());
      final Instant badgeAExpiration = Instant.now().plus(Duration.ofDays(21)).truncatedTo(ChronoUnit.SECONDS);

      final Instant account3Device1Created = Instant.ofEpochSecond(1639923487); // 2021-12-19T14:18:07Z
      final Instant account3Device1LastSeen = Instant.ofEpochMilli(Util.todayInMillis());
      final Instant badgeBExpiration = Instant.now().plus(Duration.ofDays(21)).truncatedTo(ChronoUnit.SECONDS);
      final Instant badgeCExpiration = Instant.now().plus(Duration.ofDays(24)).truncatedTo(ChronoUnit.SECONDS);

      return Stream.of(
          Arguments.of(
              buildTestAccountForDataReport(UUID.randomUUID(), exampleNumber1,
                  true, true,
                  Collections.emptyList(),
                  List.of(new DeviceData(Device.PRIMARY_ID, account1Device1LastSeen, account1Device1Created, null),
                      new DeviceData((byte) 2, account1Device2LastSeen, account1Device2Created, "OWP"))),
              String.format("""
                      # Account
                      Phone number: %s
                      Allow sealed sender from anyone: true
                      Find account by phone number: true
                      Badges: None
                                        
                      # Devices
                      - ID: 1
                        Created: 2022-11-24T20:52:22Z
                        Last seen: %s
                        User-agent: null
                      - ID: 2
                        Created: 2023-03-18T15:58:42Z
                        Last seen: 2023-03-15T00:00:00Z
                        User-agent: OWP
                      """,
                  exampleNumber1,
                  account1Device1LastSeen)
          ),
          Arguments.of(
              buildTestAccountForDataReport(UUID.randomUUID(), account2PhoneNumber,
                  false, true,
                  List.of(new AccountBadge("badge_a", badgeAExpiration, true)),
                  List.of(new DeviceData(Device.PRIMARY_ID, account2Device1LastSeen, account2Device1Created, "OWI"))),
              String.format("""
                      # Account
                      Phone number: %s
                      Allow sealed sender from anyone: false
                      Find account by phone number: true
                      Badges:
                      - ID: badge_a
                        Expiration: %s
                        Visible: true
                                        
                      # Devices
                      - ID: 1
                        Created: 2022-07-29T19:30:01Z
                        Last seen: %s
                        User-agent: OWI
                      """, account2PhoneNumber,
                  badgeAExpiration,
                  account2Device1LastSeen)
          ),
          Arguments.of(
              buildTestAccountForDataReport(UUID.randomUUID(), account3PhoneNumber,
                  true, false,
                  List.of(
                      new AccountBadge("badge_b", badgeBExpiration, true),
                      new AccountBadge("badge_c", badgeCExpiration, false)),
                  List.of(new DeviceData(Device.PRIMARY_ID, account3Device1LastSeen, account3Device1Created, "OWA"))),
              String.format("""
                      # Account
                      Phone number: %s
                      Allow sealed sender from anyone: true
                      Find account by phone number: false
                      Badges:
                      - ID: badge_b
                        Expiration: %s
                        Visible: true
                      - ID: badge_c
                        Expiration: %s
                        Visible: false
                                        
                      # Devices
                      - ID: 1
                        Created: 2021-12-19T14:18:07Z
                        Last seen: %s
                        User-agent: OWA
                      """, account3PhoneNumber,
                  badgeBExpiration,
                  badgeCExpiration,
                  account3Device1LastSeen)
          )
      );
    }

    /**
     * Creates an {@link Account} with data sufficient for
     * {@link AccountControllerV2#getAccountDataReport(AuthenticatedDevice)}.
     * <p>
     * Note: All devices will have a {@link SaltedTokenHash} for "password"
     */
    static Account buildTestAccountForDataReport(final UUID aci, final String number,
        final boolean unrestrictedUnidentifiedAccess, final boolean discoverableByPhoneNumber,
        List<AccountBadge> badges, List<DeviceData> devices) {

      final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
      final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

      final Account account = new Account();
      account.setUuid(aci);
      account.setNumber(number, UUID.randomUUID());
      account.setUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess);
      account.setDiscoverableByPhoneNumber(discoverableByPhoneNumber);
      account.setBadges(Clock.systemUTC(), new ArrayList<>(badges));
      account.setIdentityKey(new IdentityKey(aciIdentityKeyPair.getPublicKey()));
      account.setPhoneNumberIdentityKey(new IdentityKey(pniIdentityKeyPair.getPublicKey()));

      assert !devices.isEmpty();

      final SaltedTokenHash passwordTokenHash = SaltedTokenHash.generateFor("password");

      devices.forEach(deviceData -> {
        final Device device = new Device();
        device.setId(deviceData.id);
        device.setAuthTokenHash(passwordTokenHash);
        device.setFetchesMessages(true);
        device.setLastSeen(deviceData.lastSeen().toEpochMilli());
        device.setCreated(deviceData.created().toEpochMilli());
        device.setUserAgent(deviceData.userAgent());
        account.addDevice(device);
      });

      return account;
    }

    private record DeviceData(byte id, Instant lastSeen, Instant created, @Nullable String userAgent) {

    }

    private static String toE164(Phonenumber.PhoneNumber phoneNumber) {
      return PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164);
    }
  }
}
