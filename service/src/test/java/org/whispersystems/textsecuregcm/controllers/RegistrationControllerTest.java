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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
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
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockError;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class RegistrationControllerTest {

  private static final long SESSION_EXPIRATION_SECONDS = Duration.ofMinutes(10).toSeconds();

  private static final String NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);
  private static final String PASSWORD = "password";

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager = mock(
      RegistrationLockVerificationManager.class);
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private final Keys keys = mock(Keys.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);

  private final RateLimiter registrationLimiter = mock(RateLimiter.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(
          new RegistrationController(accountsManager,
              new PhoneVerificationTokenManager(registrationServiceClient, registrationRecoveryPasswordsManager),
              registrationLockVerificationManager, keys, rateLimiters))
      .build();

  @BeforeEach
  void setUp() {
    when(rateLimiters.getRegistrationLimiter()).thenReturn(registrationLimiter);
  }

  @Test
  public void testRegistrationRequest() throws Exception {
    assertFalse(new RegistrationRequest("", new byte[0], new AccountAttributes(), true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()).isValid());
    assertFalse(new RegistrationRequest("some", new byte[32], new AccountAttributes(), true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()).isValid());
    assertTrue(new RegistrationRequest("", new byte[32], new AccountAttributes(), true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()).isValid());
    assertTrue(new RegistrationRequest("some", new byte[0], new AccountAttributes(), true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()).isValid());
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
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(invalidRequestJson()))) {
      assertEquals(422, response.getStatus());
    }
  }

  @Test
  void rateLimitedNumber() throws Exception {
    doThrow(RateLimitExceededException.class)
        .when(registrationLimiter).validate(NUMBER);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(429, response.getStatus());
    }
  }

  @Test
  void registrationServiceTimeout() {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
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
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJsonRecoveryPassword(new byte[32])))) {
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
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(expectedStatus, response.getStatus(), message);
    }
  }

  static Stream<Arguments> registrationServiceSessionCheck() {
    return Stream.of(
        Arguments.of(null, 401, "session not found"),
        Arguments.of(
            new RegistrationServiceSession(new byte[16], "+18005551234", false, null, null, null,
                SESSION_EXPIRATION_SECONDS),
            400,
            "session number mismatch"),
        Arguments.of(
            new RegistrationServiceSession(new byte[16], NUMBER, false, null, null, null, SESSION_EXPIRATION_SECONDS),
            401,
            "session not verified")
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
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    final byte[] recoveryPassword = new byte[32];
    try (Response response = request.post(Entity.json(requestJsonRecoveryPassword(recoveryPassword)))) {
      assertEquals(200, response.getStatus());
    }
  }

  @Test
  void recoveryPasswordManagerVerificationFalse() throws InterruptedException {
    when(registrationRecoveryPasswordsManager.verify(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(false));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJsonRecoveryPassword(new byte[32])))) {
      assertEquals(403, response.getStatus());
    }
  }

  @ParameterizedTest
  @EnumSource(RegistrationLockError.class)
  void registrationLock(final RegistrationLockError error) throws Exception {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Optional.of(new RegistrationServiceSession(new byte[16], NUMBER, true, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));

    when(accountsManager.getByE164(any())).thenReturn(Optional.of(mock(Account.class)));

    final Exception e = switch (error) {
      case MISMATCH -> new WebApplicationException(error.getExpectedStatus());
      case RATE_LIMITED -> new RateLimitExceededException(null, true);
    };
    doThrow(e)
        .when(registrationLockVerificationManager).verifyRegistrationLock(any(), any(), any(), any(), any());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
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
        .thenReturn(
            CompletableFuture.completedFuture(
                Optional.of(new RegistrationServiceSession(new byte[16], NUMBER, true, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));

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
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJson("sessionId", new byte[0], skipDeviceTransfer)))) {
      assertEquals(expectedStatus, response.getStatus());
    }
  }

  // this is functionally the same as deviceTransferAvailable(existingAccount=false)
  @Test
  void registrationSuccess() throws Exception {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Optional.of(new RegistrationServiceSession(new byte[16], NUMBER, true, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));
    when(accountsManager.create(any(), any(), any(), any(), any()))
        .thenReturn(mock(Account.class));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(200, response.getStatus());
    }
  }

  @ParameterizedTest
  @MethodSource
  void atomicAccountCreationConflictingChannel(final RegistrationRequest conflictingChannelRequest) {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Optional.of(new RegistrationServiceSession(new byte[16], NUMBER, true, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD))
        .post(Entity.json(conflictingChannelRequest))) {

      assertEquals(422, response.getStatus());
    }
  }

  static Stream<Arguments> atomicAccountCreationConflictingChannel() {
    final Optional<String> aciIdentityKey;
    final Optional<String> pniIdentityKey;
    final Optional<SignedPreKey> aciSignedPreKey;
    final Optional<SignedPreKey> pniSignedPreKey;
    final Optional<SignedPreKey> aciPqLastResortPreKey;
    final Optional<SignedPreKey> pniPqLastResortPreKey;
    {
      final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
      final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

      aciIdentityKey = Optional.of(KeysHelper.serializeIdentityKey(aciIdentityKeyPair));
      pniIdentityKey = Optional.of(KeysHelper.serializeIdentityKey(pniIdentityKeyPair));
      aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
      pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
      aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
      pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));
    }

    final AccountAttributes fetchesMessagesAccountAttributes =
        new AccountAttributes(true, 1, "test", null, true, new Device.DeviceCapabilities());

    final AccountAttributes pushAccountAttributes =
        new AccountAttributes(false, 1, "test", null, true, new Device.DeviceCapabilities());

    return Stream.of(
        // "Fetches messages" is true, but an APNs token is provided
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            fetchesMessagesAccountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.of(new ApnRegistrationId("apns-token", null)),
            Optional.empty())),

        // "Fetches messages" is true, but an FCM (GCM) token is provided
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            fetchesMessagesAccountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.empty(),
            Optional.of(new GcmRegistrationId("gcm-token")))),

        // "Fetches messages" is false, but multiple types of push tokens are provided
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            pushAccountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.of(new ApnRegistrationId("apns-token", null)),
            Optional.of(new GcmRegistrationId("gcm-token"))))
    );
  }

  @ParameterizedTest
  @MethodSource
  void atomicAccountCreationPartialSignedPreKeys(final RegistrationRequest partialSignedPreKeyRequest) {
    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Optional.of(new RegistrationServiceSession(new byte[16], NUMBER, true, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (final Response response = request.post(Entity.json(partialSignedPreKeyRequest))) {
      assertEquals(422, response.getStatus());
    }
  }

  static Stream<Arguments> atomicAccountCreationPartialSignedPreKeys() {
    final Optional<String> aciIdentityKey;
    final Optional<String> pniIdentityKey;
    final Optional<SignedPreKey> aciSignedPreKey;
    final Optional<SignedPreKey> pniSignedPreKey;
    final Optional<SignedPreKey> aciPqLastResortPreKey;
    final Optional<SignedPreKey> pniPqLastResortPreKey;
    {
      final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
      final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

      aciIdentityKey = Optional.of(KeysHelper.serializeIdentityKey(aciIdentityKeyPair));
      pniIdentityKey = Optional.of(KeysHelper.serializeIdentityKey(pniIdentityKeyPair));
      aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
      pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
      aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
      pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));
    }

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, 1, "test", null, true, new Device.DeviceCapabilities());

    return Stream.of(
        // Signed PNI EC pre-key is missing
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            accountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.empty(),
            Optional.empty())),

        // Signed ACI EC pre-key is missing
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            accountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            Optional.empty(),
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.empty(),
            Optional.empty())),

        // Signed PNI KEM pre-key is missing
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            accountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty())),

        // Signed ACI KEM pre-key is missing
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            accountAttributes,
            true,
            aciIdentityKey,
            pniIdentityKey,
            aciSignedPreKey,
            pniSignedPreKey,
            Optional.empty(),
            pniPqLastResortPreKey,
            Optional.empty(),
            Optional.empty())),

        // All signed pre-keys are present, but ACI identity key is missing
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            accountAttributes,
            true,
            Optional.empty(),
            pniIdentityKey,
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.empty(),
            Optional.empty())),

        // All signed pre-keys are present, but PNI identity key is missing
        Arguments.of(new RegistrationRequest("session-id",
            new byte[0],
            accountAttributes,
            true,
            aciIdentityKey,
            Optional.empty(),
            aciSignedPreKey,
            pniSignedPreKey,
            aciPqLastResortPreKey,
            pniPqLastResortPreKey,
            Optional.empty(),
            Optional.empty()))
    );
  }


  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  void atomicAccountCreationSuccess(final RegistrationRequest registrationRequest,
      final String expectedAciIdentityKey,
      final String expectedPniIdentityKey,
      final SignedPreKey expectedAciSignedPreKey,
      final SignedPreKey expectedPniSignedPreKey,
      final SignedPreKey expectedAciPqLastResortPreKey,
      final SignedPreKey expectedPniPqLastResortPreKey,
      final Optional<String> expectedApnsToken,
      final Optional<String> expectedApnsVoipToken,
      final Optional<String> expectedGcmToken) throws InterruptedException {

    when(registrationServiceClient.getSession(any(), any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                Optional.of(new RegistrationServiceSession(new byte[16], NUMBER, true, null, null, null,
                    SESSION_EXPIRATION_SECONDS))));

    final UUID accountIdentifier = UUID.randomUUID();
    final UUID phoneNumberIdentifier = UUID.randomUUID();

    final Account account = MockUtils.buildMock(Account.class, a -> {
      when(a.getUuid()).thenReturn(accountIdentifier);
      when(a.getPhoneNumberIdentifier()).thenReturn(phoneNumberIdentifier);
    });

    final Device device = mock(Device.class);

    when(accountsManager.create(any(), any(), any(), any(), any()))
        .thenReturn(account);

    when(accountsManager.update(eq(account), any())).thenAnswer(invocation -> {
      final Consumer<Account> accountUpdater = invocation.getArgument(1);
      accountUpdater.accept(account);

      return invocation.getArgument(0);
    });

    when(accountsManager.updateDevice(eq(account), eq(Device.MASTER_ID), any())).thenAnswer(invocation -> {
      final Consumer<Device> deviceUpdater = invocation.getArgument(2);
      deviceUpdater.accept(device);

      return invocation.getArgument(0);
    });

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(registrationRequest))) {
      assertEquals(200, response.getStatus());
    }

    verify(accountsManager).create(any(), any(), any(), any(), any());
    verify(accountsManager).updateDevice(eq(account), eq(Device.MASTER_ID), any());

    verify(account).setIdentityKey(expectedAciIdentityKey);
    verify(account).setPhoneNumberIdentityKey(expectedPniIdentityKey);

    verify(device).setSignedPreKey(expectedAciSignedPreKey);
    verify(device).setPhoneNumberIdentitySignedPreKey(expectedPniSignedPreKey);

    verify(keys).storePqLastResort(accountIdentifier, Map.of(Device.MASTER_ID, expectedAciPqLastResortPreKey));
    verify(keys).storePqLastResort(phoneNumberIdentifier, Map.of(Device.MASTER_ID, expectedPniPqLastResortPreKey));

    expectedApnsToken.ifPresentOrElse(expectedToken -> verify(device).setApnId(expectedToken),
        () -> verify(device, never()).setApnId(any()));

    expectedApnsVoipToken.ifPresentOrElse(expectedToken -> verify(device).setVoipApnId(expectedToken),
        () -> verify(device, never()).setVoipApnId(any()));

    expectedGcmToken.ifPresentOrElse(expectedToken -> verify(device).setGcmId(expectedToken),
        () -> verify(device, never()).setGcmId(any()));
  }

  private static Stream<Arguments> atomicAccountCreationSuccess() {
    final Optional<String> aciIdentityKey;
    final Optional<String> pniIdentityKey;
    final Optional<SignedPreKey> aciSignedPreKey;
    final Optional<SignedPreKey> pniSignedPreKey;
    final Optional<SignedPreKey> aciPqLastResortPreKey;
    final Optional<SignedPreKey> pniPqLastResortPreKey;
    {
      final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
      final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

      aciIdentityKey = Optional.of(KeysHelper.serializeIdentityKey(aciIdentityKeyPair));
      pniIdentityKey = Optional.of(KeysHelper.serializeIdentityKey(pniIdentityKeyPair));
      aciSignedPreKey = Optional.of(KeysHelper.signedECPreKey(1, aciIdentityKeyPair));
      pniSignedPreKey = Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair));
      aciPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair));
      pniPqLastResortPreKey = Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));
    }

    final AccountAttributes fetchesMessagesAccountAttributes =
        new AccountAttributes(true, 1, "test", null, true, new Device.DeviceCapabilities());

    final AccountAttributes pushAccountAttributes =
        new AccountAttributes(false, 1, "test", null, true, new Device.DeviceCapabilities());

    final String apnsToken = "apns-token";
    final String apnsVoipToken = "apns-voip-token";
    final String gcmToken = "gcm-token";

    return Stream.of(
        // Fetches messages; no push tokens
        Arguments.of(new RegistrationRequest("session-id",
                new byte[0],
                fetchesMessagesAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                aciSignedPreKey,
                pniSignedPreKey,
                aciPqLastResortPreKey,
                pniPqLastResortPreKey,
                Optional.empty(),
                Optional.empty()),
            aciIdentityKey.get(),
            pniIdentityKey.get(),
            aciSignedPreKey.get(),
            pniSignedPreKey.get(),
            aciPqLastResortPreKey.get(),
            pniPqLastResortPreKey.get(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()),

        // Has APNs tokens
        Arguments.of(new RegistrationRequest("session-id",
                new byte[0],
                pushAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                aciSignedPreKey,
                pniSignedPreKey,
                aciPqLastResortPreKey,
                pniPqLastResortPreKey,
                Optional.of(new ApnRegistrationId(apnsToken, apnsVoipToken)),
                Optional.empty()),
            aciIdentityKey.get(),
            pniIdentityKey.get(),
            aciSignedPreKey.get(),
            pniSignedPreKey.get(),
            aciPqLastResortPreKey.get(),
            pniPqLastResortPreKey.get(),
            Optional.of(apnsToken),
            Optional.of(apnsVoipToken),
            Optional.empty()),

        // Fetches messages; no push tokens
        Arguments.of(new RegistrationRequest("session-id",
                new byte[0],
                pushAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                aciSignedPreKey,
                pniSignedPreKey,
                aciPqLastResortPreKey,
                pniPqLastResortPreKey,
                Optional.empty(),
                Optional.of(new GcmRegistrationId(gcmToken))),
            aciIdentityKey.get(),
            pniIdentityKey.get(),
            aciSignedPreKey.get(),
            pniSignedPreKey.get(),
            aciPqLastResortPreKey.get(),
            pniPqLastResortPreKey.get(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(gcmToken)));
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

  private static String encodeSessionId(final String sessionId) {
    return Base64.getUrlEncoder().encodeToString(sessionId.getBytes(StandardCharsets.UTF_8));
  }

  private static String encodeRecoveryPassword(final byte[] recoveryPassword) {
    return Base64.getEncoder().encodeToString(recoveryPassword);
  }
}
