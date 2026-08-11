/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.storage.ReceiptCredentialTestUtil.receiptPresentation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.InvalidRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RecoveryPasswordVerificationFailedException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockError;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.UnverifiedRegistrationSessionException;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicLoginPurchaseConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountCreationResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeviceActivationRequest;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.spam.RegistrationFraudChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DeviceIdentityInfo;
import org.whispersystems.textsecuregcm.storage.DeviceSpec;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptLevel;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
class RegistrationControllerTest {

  private static final String NUMBER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164);
  private static final String PASSWORD = "password";
  private static final String REGLOCK = RandomStringUtils.insecure().nextAlphanumeric(64);

  private final AccountsManager accountsManager = mock(AccountsManager.class);
  private final RegistrationLockVerificationManager registrationLockVerificationManager
      = mock(RegistrationLockVerificationManager.class);
  private final RateLimiters rateLimiters = mock(RateLimiters.class);
  private final RegistrationFraudChecker registrationFraudChecker = mock(RegistrationFraudChecker.class);
  private final PhoneVerificationTokenManager phoneVerificationTokenManager = mock(PhoneVerificationTokenManager.class);

  private final RateLimiter registrationLimiter = mock(RateLimiter.class);

  private static final Clock CLOCK = TestClock.pinned(Instant.now());

  private static final DynamicLoginPurchaseConfiguration ENABLED = new DynamicLoginPurchaseConfiguration(true);
  private static final DynamicLoginPurchaseConfiguration DISABLED = new DynamicLoginPurchaseConfiguration(false);

  @SuppressWarnings("unchecked")
  private static final DynamicConfigurationManager<DynamicConfiguration> DYNAMIC_CONFIGURATION_MANAGER =
      mock(DynamicConfigurationManager.class);
  private static final DynamicConfiguration DYNAMIC_CONFIGURATION = mock(DynamicConfiguration.class);

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new RegistrationController(accountsManager, phoneVerificationTokenManager,
          registrationLockVerificationManager, rateLimiters, registrationFraudChecker, ReceiptCredentialPresentation::new, mock(
          ServerZkReceiptOperations.class), CLOCK, DYNAMIC_CONFIGURATION_MANAGER))
      .build();

  @BeforeEach
  void setUp() throws Exception {
    when(rateLimiters.getRegistrationLimiter()).thenReturn(registrationLimiter);

    when(accountsManager.update(any(UUID.class), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      final Consumer<Account> accountUpdater = invocation.getArgument(1);

      accountUpdater.accept(account);

      return invocation.getArgument(0);
    });


    reset(DYNAMIC_CONFIGURATION_MANAGER, DYNAMIC_CONFIGURATION);

    when(DYNAMIC_CONFIGURATION_MANAGER.getConfiguration()).thenReturn(DYNAMIC_CONFIGURATION);
    when(DYNAMIC_CONFIGURATION.getLoginPurchaseConfiguration()).thenReturn(ENABLED);

    reset(registrationFraudChecker);
    reset(phoneVerificationTokenManager);

    when(phoneVerificationTokenManager.verify(any(), any(), any(), any(), any(), any())).thenAnswer(invocation -> {
      final byte[] sessionId = invocation.getArgument(4);

      return sessionId != null
          ? PhoneVerificationRequest.VerificationType.SESSION
          : PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD;
    });
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

  static Stream<Arguments> invalidRegistrationId() {
    return Stream.of(
        Arguments.of(Optional.of(1), Optional.of(1), 200),
        Arguments.of(Optional.of(1), Optional.empty(), 422),
        Arguments.of(Optional.of(0x3FFF), Optional.empty(), 422),
        Arguments.of(Optional.empty(), Optional.of(1), 422),
        Arguments.of(Optional.of(Integer.MAX_VALUE), Optional.empty(), 422),
        Arguments.of(Optional.of(0x3FFF + 1), Optional.empty(), 422),
        Arguments.of(Optional.of(1), Optional.of(0x3FFF + 1), 422)
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void invalidRegistrationId(Optional<Integer> registrationId, Optional<Integer> pniRegistrationId, int statusCode) {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    final Account account = mock(Account.class);
    when(account.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(account);

    final String json = requestJson("sessionId", new byte[0], true, registrationId.orElse(0), pniRegistrationId.orElse(null));

    try (Response response = request.post(Entity.json(json))) {
      assertEquals(statusCode, response.getStatus());
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

  @ParameterizedTest
  @MethodSource
  void phoneVerificationException(final Exception exception, final int expectedStatus) throws Exception {
    doThrow(exception)
        .when(phoneVerificationTokenManager).verify(any(), any(), any(), any(), any(), any());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(expectedStatus, response.getStatus());
    }
  }

  private static List<Arguments> phoneVerificationException() {
    return List.of(
        Arguments.argumentSet("Bad request", new InvalidRegistrationSessionException("invalid registration session"), HttpStatus.SC_BAD_REQUEST),
        Arguments.argumentSet("Not authorized", new UnverifiedRegistrationSessionException(), HttpStatus.SC_UNAUTHORIZED),
        Arguments.argumentSet("Forbidden", new RecoveryPasswordVerificationFailedException(), HttpStatus.SC_FORBIDDEN),
        Arguments.argumentSet("Unexpected exception", new IOException("unavailable"), HttpStatus.SC_SERVICE_UNAVAILABLE)
    );
  }

  @CartesianTest
  @CartesianTest.MethodFactory("registrationLockAndDeviceTransfer")
  void registrationLockAndDeviceTransfer(
      final boolean deviceTransferSupported,
      @Nullable final RegistrationLockError error) throws Exception {

    final Account account = mock(Account.class);
    when(accountsManager.getByE164(any())).thenReturn(Optional.of(account));
    when(account.getNumberOptional()).thenReturn(Optional.of(NUMBER));
    when(account.hasCapability(DeviceCapability.TRANSFER)).thenReturn(deviceTransferSupported);

    final int expectedStatus;
    if (deviceTransferSupported) {
      expectedStatus = 409;
    } else if (error != null) {
      final Exception e = switch (error) {
        case MISMATCH -> new WebApplicationException(error.getExpectedStatus());
        case RATE_LIMITED -> new RateLimitExceededException(null);
      };
      doThrow(e)
          .when(registrationLockVerificationManager).verifyRegistrationLock(any(), any(), any(), any(), any());
      expectedStatus = error.getExpectedStatus();
    } else {
      final Account createdAccount = mock(Account.class);
      when(createdAccount.getPrimaryDevice()).thenReturn(mock(Device.class));

      when(accountsManager.create(any(), any(), any(), any(), any(), any()))
          .thenReturn(createdAccount);

      expectedStatus = 200;
    }

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(expectedStatus, response.getStatus());
    }
  }

  @SuppressWarnings("unused")
  static ArgumentSets registrationLockAndDeviceTransfer() {
    final Set<RegistrationLockError> registrationLockErrors = new HashSet<>(EnumSet.allOf(RegistrationLockError.class));
    registrationLockErrors.add(null);

    return ArgumentSets.argumentsForFirstParameter(true, false)
        .argumentsForNextParameter(registrationLockErrors);
  }

  @Test
  void registrationLockOnAlternatePhoneNumberForm() throws Exception {
    final String newFormatBeninNumber = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final String oldFormatBeninNumber = newFormatBeninNumber.replaceFirst("01", "");

    assertNotEquals(newFormatBeninNumber, oldFormatBeninNumber);

    final Account account = mock(Account.class);
    when(accountsManager.getByE164(oldFormatBeninNumber)).thenReturn(Optional.of(account));
    when(account.getNumberOptional()).thenReturn(Optional.of(oldFormatBeninNumber));
    when(accountsManager.getByE164(newFormatBeninNumber)).thenReturn(Optional.empty());

    doThrow(new WebApplicationException(RegistrationLockError.MISMATCH.getExpectedStatus()))
        .when(registrationLockVerificationManager).verifyRegistrationLock(any(), any(), any(), any(), any());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(newFormatBeninNumber, PASSWORD));

    try (final Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(RegistrationLockError.MISMATCH.getExpectedStatus(), response.getStatus());
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
      final boolean skipDeviceTransfer, final int expectedStatus) {

    final Optional<Account> maybeAccount;
    if (existingAccount) {
      final Account account = mock(Account.class);
      when(account.hasCapability(DeviceCapability.TRANSFER)).thenReturn(transferSupported);
      when(account.getNumberOptional()).thenReturn(Optional.of(NUMBER));
      maybeAccount = Optional.of(account);
    } else {
      maybeAccount = Optional.empty();
    }
    when(accountsManager.getByE164(any())).thenReturn(maybeAccount);

    final Account account = mock(Account.class);
    when(account.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    try (Response response = request.post(Entity.json(requestJson("sessionId", new byte[0], skipDeviceTransfer, 1, 2)))) {
      assertEquals(expectedStatus, response.getStatus());
    }
  }

  // this is functionally the same as deviceTransferAvailable(existingAccount=false)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void registrationSuccess(final boolean useSessionVerification) {
    final Account account = mock(Account.class);
    when(account.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    final String requestJson = useSessionVerification
        ? requestJson("sessionId")
        : requestJsonRecoveryPassword("recovery-password".getBytes(StandardCharsets.UTF_8));

    try (Response response = request.post(Entity.json(requestJson))) {
      assertEquals(200, response.getStatus());
    }

    if (useSessionVerification) {
      verify(registrationFraudChecker)
          .handleVerificationCompleted(Base64.getEncoder().encodeToString("sessionId".getBytes(StandardCharsets.UTF_8)),
              account);
    }
  }

  @ParameterizedTest
  @MethodSource
  void atomicAccountCreationConflictingChannel(final RegistrationRequest conflictingChannelRequest) {
    try (final Response response = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD))
        .post(Entity.json(conflictingChannelRequest))) {

      assertEquals(422, response.getStatus());
    }
  }

  static List<Arguments> atomicAccountCreationConflictingChannel() {
    final IdentityKey aciIdentityKey;
    final IdentityKey pniIdentityKey;
    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;
    {
      final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
      final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

      aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
      pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
      aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
      pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
      aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
      pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);
    }

    final AccountAttributes fetchesMessagesAccountAttributes =
        new AccountAttributes(true, 1, 1, "test".getBytes(StandardCharsets.UTF_8), null, true,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null);

    final AccountAttributes pushAccountAttributes =
        new AccountAttributes(false, 1, 1, "test".getBytes(StandardCharsets.UTF_8), null, true,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null);

    return List.of(
        Arguments.argumentSet("\"Fetches messages\" is true, but an APNs token is provided",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                fetchesMessagesAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.of(new ApnRegistrationId("apns-token")),
                    Optional.empty()))),

        Arguments.argumentSet("\"Fetches messages\" is true, but an FCM (GCM) token is provided",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                fetchesMessagesAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.of(new GcmRegistrationId("gcm-token"))))),

        Arguments.argumentSet("\"Fetches messages\" is false, but multiple types of push tokens are provided",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                pushAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.of(new ApnRegistrationId("apns-token")),
                    Optional.of(new GcmRegistrationId("gcm-token")))))
    );
  }

  @ParameterizedTest
  @MethodSource
  void atomicAccountCreationPartialSignedPreKeys(final RegistrationRequest partialSignedPreKeyRequest) {
    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (final Response response = request.post(Entity.json(partialSignedPreKeyRequest))) {
      assertEquals(422, response.getStatus());
    }
  }

  static List<Arguments> atomicAccountCreationPartialSignedPreKeys() {
    final IdentityKey aciIdentityKey;
    final IdentityKey pniIdentityKey;
    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;
    {
      final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
      final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

      aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
      pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
      aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
      pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
      aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
      pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);
    }

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, 1, 1, "test".getBytes(StandardCharsets.UTF_8), null, true,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null);

    return List.of(
        Arguments.argumentSet("Signed PNI EC pre-key is missing",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                accountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    null,
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.empty()))),

        Arguments.argumentSet("Signed ACI EC pre-key is missing",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                accountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(null,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.empty()))),

        Arguments.argumentSet("Signed PNI KEM pre-key is missing",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                accountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    null,
                    Optional.empty(),
                    Optional.empty()))),

        Arguments.argumentSet("Signed ACI KEM pre-key is missing",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                accountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    null,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.empty()))),

        Arguments.argumentSet("All signed pre-keys are present, but ACI identity key is missing",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                accountAttributes,
                true,
                null,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.empty()))),

        Arguments.argumentSet("All signed pre-keys are present, but PNI identity key is missing",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                accountAttributes,
                true,
                aciIdentityKey,
                null,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.empty())))
    );
  }


  @ParameterizedTest
  @MethodSource
  void atomicAccountCreationSuccess(final RegistrationRequest registrationRequest,
      final IdentityKey expectedAciIdentityKey,
      final IdentityKey expectedPniIdentityKey,
      final DeviceSpec expectedDeviceSpec) {

    final UUID accountIdentifier = UUID.randomUUID();
    final UUID phoneNumberIdentifier = UUID.randomUUID();
    final Device device = mock(Device.class);

    final Account account = MockUtils.buildMock(Account.class, a -> {
      when(a.getAccountIdentifier()).thenReturn(accountIdentifier);
      when(a.getPhoneNumberIdentifierOptional()).thenReturn(Optional.of(phoneNumberIdentifier));
      when(a.getPrimaryDevice()).thenReturn(device);
    });

    when(accountsManager.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(registrationRequest))) {
      assertEquals(200, response.getStatus());
      final AccountIdentityResponse identityResponse = response.readEntity(AccountIdentityResponse.class);
      assertEquals(accountIdentifier, identityResponse.uuid());
    }

    verify(accountsManager).create(
        eq(NUMBER),
        argThat(attributes -> accountAttributesEqual(attributes, registrationRequest.accountAttributes())),
        eq(expectedAciIdentityKey),
        eq(expectedPniIdentityKey),
        eq(expectedDeviceSpec),
        any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void reregistrationFlag(final boolean accountExists) {
    final Account existingAccount = mock(Account.class);
    when(existingAccount.getNumberOptional()).thenReturn(Optional.of(NUMBER));
    when(accountsManager.getByE164(any())).thenReturn(accountExists ? Optional.of(existingAccount) : Optional.empty());

    final Account account = mock(Account.class);
    when(account.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestJson("sessionId")))) {
      assertEquals(200, response.getStatus());
      final AccountCreationResponse creationResponse = response.readEntity(AccountCreationResponse.class);
      assertEquals(accountExists, creationResponse.reregistration());
    }
  }

  @Test
  void registrationMissingSpqrCapability() {
    final Account account = mock(Account.class);
    when(account.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));
    final RegistrationRequest requestObj = request("sessionId", new byte[0], false, 1, 2, Collections.emptySet());
    try (final Response response = request.post(Entity.json(requestToJson(requestObj)))) {
      assertEquals(499, response.getStatus());
    }
  }

  @Test
  void registerAccountWithoutNumber() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final byte[] deviceName = "test".getBytes(StandardCharsets.UTF_8);
    final int registrationId = 1;
    final Set<DeviceCapability> deviceCapabilities = DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES;

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, registrationId, null, deviceName, null, false, deviceCapabilities, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    final ReceiptCredentialPresentation receiptCredentialPresentation =
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), ReceiptLevel.LOGIN.getValue());

    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        new byte[0],
        receiptCredentialPresentation.serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = MockUtils.buildMock(Account.class,
        a -> when(a.getAccountIdentifier()).thenReturn(accountIdentifier));

    when(accountsManager.create(any(), any(), any(), any(), any())).thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(200, response.getStatus());

      final AccountCreationResponse creationResponse = response.readEntity(AccountCreationResponse.class);
      assertEquals(accountIdentifier, creationResponse.identityResponse().uuid());
      assertEquals(Optional.empty(), creationResponse.identityResponse().number());
      assertEquals(Optional.empty(), creationResponse.identityResponse().pni());
      assertFalse(creationResponse.reregistration());
    }

    verify(accountsManager).create(
        argThat(attributes -> accountAttributesEqual(attributes, accountAttributes)),
        eq(aciIdentityKey),
        argThat(presentation ->
            Arrays.equals(presentation.serialize(), receiptCredentialPresentation.serialize())),
        eq(new DeviceSpec(
            deviceName,
            PASSWORD,
            null,
            deviceCapabilities,
            new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
            Optional.empty(),
            true,
            Optional.empty(),
            Optional.empty())),
        any());

    verifyNoInteractions(phoneVerificationTokenManager);
  }

  @Test
  void registerAccountWithoutNumberLoginPurchasesDisabled() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final byte[] deviceName = "test".getBytes(StandardCharsets.UTF_8);
    final int registrationId = 1;
    final Set<DeviceCapability> deviceCapabilities = DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES;

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, registrationId, null, deviceName, null, false, deviceCapabilities, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    final ReceiptCredentialPresentation receiptCredentialPresentation =
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), ReceiptLevel.LOGIN.getValue());

    when(DYNAMIC_CONFIGURATION.getLoginPurchaseConfiguration()).thenReturn(DISABLED);
    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        new byte[0],
        receiptCredentialPresentation.serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(400, response.getStatus());
    }

    verifyNoInteractions(accountsManager);
  }

  @ParameterizedTest
  @MethodSource
  void registerAccountBadReceipt(final byte[] receiptCredentialPresentation) {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final byte[] deviceName = "test".getBytes(StandardCharsets.UTF_8);
    final int registrationId = 1;
    final Set<DeviceCapability> deviceCapabilities = DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES;

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, registrationId, null, deviceName, null, false, deviceCapabilities, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        new byte[0],
        receiptCredentialPresentation,
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = MockUtils.buildMock(Account.class,
        a -> when(a.getAccountIdentifier()).thenReturn(accountIdentifier));

    when(accountsManager.create(any(), any(), any(), any(), any(), any())).thenReturn(account);

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(400, response.getStatus());
    }

    verifyNoInteractions(phoneVerificationTokenManager);
    verifyNoInteractions(accountsManager);
  }

  static Stream<Arguments> registerAccountBadReceipt() throws InvalidInputException, VerificationFailedException {
    return Stream.of(
        Arguments.argumentSet("malformed receipt", new byte[]{0, 0}),
        Arguments.argumentSet("expired receipt", receiptPresentation(CLOCK.instant().minusSeconds(5), ReceiptLevel.LOGIN.getValue()).serialize()),
        Arguments.argumentSet("wrong receipt level", receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), ReceiptLevel.BACKUP_PAID.getValue()).serialize())
    );
  }

  @Test
  void registerAccountSessionIdAndReceiptNotAllowed() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, 1, null, "test".getBytes(StandardCharsets.UTF_8), null, false,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    // A session ID and a receipt credential presentation are mutually exclusive
    final RegistrationRequest registrationRequest = new RegistrationRequest(
        Base64.getEncoder().encodeToString("session-id".getBytes(StandardCharsets.UTF_8)),
        new byte[0],
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), 1).serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(422, response.getStatus());
    }

    verifyNoInteractions(phoneVerificationTokenManager);
  }

  @Test
  void registerAccountReceiptAndRecoveryPasswordNotAllowed() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final byte[] recoveryPassword = TestRandomUtil.nextBytes(16);

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, 1, null, "test".getBytes(StandardCharsets.UTF_8), null, false,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    // A receipt credential presentation and recovery password are mutually exclusive
    final RegistrationRequest registrationRequest = new RegistrationRequest(
        null,
        recoveryPassword,
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), 1).serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(422, response.getStatus());
    }

  }

  @Test
  void registerAccountMissingRecoveryPassword() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, 1, null, "test".getBytes(StandardCharsets.UTF_8), null, false,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16));

    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        new byte[0],
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), 1).serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(KeysHelper.signedECPreKey(1, aciIdentityKeyPair),
            Optional.empty(),
            KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(422, response.getStatus());
    }

    verifyNoInteractions(accountsManager);
  }

  @Test
  void registerAccountWithoutNumberRejectsPniKeys() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final ECSignedPreKey pniSignedPreKey = KeysHelper.signedECPreKey(1, pniIdentityKeyPair);

    final byte[] deviceName = "test".getBytes(StandardCharsets.UTF_8);
    final int registrationId = 1;
    final Set<DeviceCapability> deviceCapabilities = DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES;

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, registrationId, null, deviceName, null, false, deviceCapabilities, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    final ReceiptCredentialPresentation receiptCredentialPresentation =
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), ReceiptLevel.LOGIN.getValue());

    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        new byte[0],
        receiptCredentialPresentation.serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.of(pniSignedPreKey),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (final Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(422, response.getStatus());
    }
  }

  @Test
  void registerAccountReceiptAlreadyRedeemed() throws Exception {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, 1, null, "test".getBytes(StandardCharsets.UTF_8), null, false,
            DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        new byte[0],
        receiptPresentation(CLOCK.instant().plus(Duration.ofDays(30)), 1).serialize(),
        accountAttributes,
        true,
        aciIdentityKey,
        null,
        new DeviceActivationRequest(KeysHelper.signedECPreKey(1, aciIdentityKeyPair),
            Optional.empty(),
            KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    when(accountsManager.create(any(), any(), any(), any(), any()))
        .thenThrow(new ReceiptAlreadyRedeemedException());

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(400, response.getStatus());
    }
  }

  @Test
  void registerAccountWithNumberMissingPniKeys() {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final ECSignedPreKey aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
    final KEMSignedPreKey aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    final byte[] deviceName = "test".getBytes(StandardCharsets.UTF_8);
    final int registrationId = 1;
    final Set<DeviceCapability> deviceCapabilities = DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES;

    final AccountAttributes accountAttributes =
        new AccountAttributes(true, registrationId, null, deviceName, null, false, deviceCapabilities, null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16))
            .setRecoveryPassword(TestRandomUtil.nextBytes(32));

    final RegistrationRequest registrationRequest = new RegistrationRequest(
        Base64.getEncoder().encodeToString("session-id".getBytes(StandardCharsets.UTF_8)),
        new byte[0],
        null,
        accountAttributes,
        true,
        aciIdentityKey,
        pniIdentityKey,
        new DeviceActivationRequest(aciSignedPreKey,
            Optional.empty(),
            aciPqLastResortPreKey,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final Invocation.Builder request = resources.getJerseyTest()
        .target("/v1/registration")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getProvisioningAuthHeader(NUMBER, PASSWORD));

    try (final Response response = request.post(Entity.json(requestToJson(registrationRequest)))) {
      assertEquals(422, response.getStatus());
    }
  }

  private static boolean accountAttributesEqual(final AccountAttributes a, final AccountAttributes b) {
    return a.getFetchesMessages() == b.getFetchesMessages()
        && a.getRegistrationId() == b.getRegistrationId()
        && a.isUnrestrictedUnidentifiedAccess() == b.isUnrestrictedUnidentifiedAccess()
        && a.isDiscoverableByPhoneNumber() == b.isDiscoverableByPhoneNumber()
        && Objects.equals(a.getPhoneNumberIdentityRegistrationId(), b.getPhoneNumberIdentityRegistrationId())
        && Arrays.equals(a.getName(), b.getName())
        && Objects.equals(a.getRegistrationLock(), b.getRegistrationLock())
        && Arrays.equals(a.getUnidentifiedAccessKey(), b.getUnidentifiedAccessKey())
        && Objects.equals(a.getCapabilities(), b.getCapabilities())
        && Arrays.equals(a.recoveryPassword().orElse(null), b.recoveryPassword().orElse(null));
  }

  private static List<Arguments> atomicAccountCreationSuccess() {
    final IdentityKey aciIdentityKey;
    final IdentityKey pniIdentityKey;
    final ECSignedPreKey aciSignedPreKey;
    final ECSignedPreKey pniSignedPreKey;
    final KEMSignedPreKey aciPqLastResortPreKey;
    final KEMSignedPreKey pniPqLastResortPreKey;
    {
      final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
      final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

      aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
      pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
      aciSignedPreKey = KeysHelper.signedECPreKey(1, aciIdentityKeyPair);
      pniSignedPreKey = KeysHelper.signedECPreKey(2, pniIdentityKeyPair);
      aciPqLastResortPreKey = KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair);
      pniPqLastResortPreKey = KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair);
    }

    final byte[] deviceName = "test".getBytes(StandardCharsets.UTF_8);
    final int registrationId = 1;
    final int pniRegistrationId = 2;

    final Set<DeviceCapability> deviceCapabilities = DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES;

    final AccountAttributes fetchesMessagesAccountAttributes =
        new AccountAttributes(true, registrationId, pniRegistrationId, "test".getBytes(StandardCharsets.UTF_8), null, true, deviceCapabilities,
            null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16));

    final AccountAttributes pushAccountAttributes =
        new AccountAttributes(false, registrationId, pniRegistrationId, "test".getBytes(StandardCharsets.UTF_8), null, true, deviceCapabilities,
            null)
            .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16));

    final String apnsToken = "apns-token";
    final String gcmToken = "gcm-token";

    return List.of(
        Arguments.argumentSet("Fetches messages; no push tokens",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                fetchesMessagesAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.empty())),
            aciIdentityKey,
            pniIdentityKey,
            new DeviceSpec(
                deviceName,
                PASSWORD,
                null,
                deviceCapabilities,
                new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
                Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
                true,
                Optional.empty(),
                Optional.empty()
            )),

        Arguments.argumentSet("Has APNs tokens",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                pushAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.of(new ApnRegistrationId(apnsToken)),
                    Optional.empty())),
            aciIdentityKey,
            pniIdentityKey,
            new DeviceSpec(
                deviceName,
                PASSWORD,
                null,
                deviceCapabilities,
                new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
                Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
                false,
                Optional.of(new ApnRegistrationId(apnsToken)),
                Optional.empty())),

        Arguments.argumentSet("Has GCM token",
            new RegistrationRequest("session-id",
                new byte[0],
                null,
                pushAccountAttributes,
                true,
                aciIdentityKey,
                pniIdentityKey,
                new DeviceActivationRequest(aciSignedPreKey,
                    Optional.of(pniSignedPreKey),
                    aciPqLastResortPreKey,
                    Optional.of(pniPqLastResortPreKey),
                    Optional.empty(),
                    Optional.of(new GcmRegistrationId(gcmToken)))),
            aciIdentityKey,
            pniIdentityKey,
            new DeviceSpec(
                deviceName,
                PASSWORD,
                null,
                deviceCapabilities,
                new DeviceIdentityInfo(registrationId, aciSignedPreKey, aciPqLastResortPreKey),
                Optional.of(new DeviceIdentityInfo(pniRegistrationId, pniSignedPreKey, pniPqLastResortPreKey)),
                false,
                Optional.empty(),
                Optional.of(new GcmRegistrationId(gcmToken))))
    );
  }

  private static RegistrationRequest request(
      final String sessionId,
      final byte[] recoveryPassword,
      final boolean skipDeviceTransfer,
      final int registrationId,
      final Integer pniRegistrationId,
      Set<DeviceCapability> deviceCapabilities) {
    final ECKeyPair aciIdentityKeyPair = ECKeyPair.generate();
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final IdentityKey aciIdentityKey = new IdentityKey(aciIdentityKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    final AccountAttributes accountAttributes = new AccountAttributes(true, registrationId, pniRegistrationId,
        "name".getBytes(StandardCharsets.UTF_8), REGLOCK,
        true, deviceCapabilities, null)
        .setUnidentifiedAccessKey(TestRandomUtil.nextBytes(16));

    return new RegistrationRequest(
        Base64.getEncoder().encodeToString(sessionId.getBytes(StandardCharsets.UTF_8)),
        recoveryPassword,
        null,
        accountAttributes,
        skipDeviceTransfer,
        aciIdentityKey,
        pniIdentityKey,
        new DeviceActivationRequest(
            KeysHelper.signedECPreKey(1, aciIdentityKeyPair),
            Optional.of(KeysHelper.signedECPreKey(2, pniIdentityKeyPair)),
            KeysHelper.signedKEMPreKey(3, aciIdentityKeyPair),
            Optional.of(KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair)),
            Optional.empty(),
            Optional.empty()));
  }

  private static String requestToJson(RegistrationRequest request) {
    try {
      return SystemMapper.jsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Valid request JSON with the give session ID and skipDeviceTransfer
   */
  private static String requestJson(final String sessionId,
      final byte[] recoveryPassword,
      final boolean skipDeviceTransfer,
      final int registrationId,
      final Integer pniRegistrationId) {
      return requestToJson(request(sessionId, recoveryPassword, skipDeviceTransfer, registrationId, pniRegistrationId, DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES));
  }

  /**
   * Valid request JSON with the given session ID
   */
  @SuppressWarnings("SameParameterValue")
  private static String requestJson(final String sessionId) {
    return requestJson(sessionId, new byte[0], false, 1, 2);
  }

  /**
   * Valid request JSON with the given Recovery Password
   */
  private static String requestJsonRecoveryPassword(final byte[] recoveryPassword) {
    return requestJson("", recoveryPassword, false, 1, 2);
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
}
