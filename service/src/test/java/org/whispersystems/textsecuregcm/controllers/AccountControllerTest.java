/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ChangePhoneNumberRequest;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameRequest;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameResponse;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.entities.UsernameRequest;
import org.whispersystems.textsecuregcm.entities.UsernameResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberResponse;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.push.PushNotification;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.registration.ClientType;
import org.whispersystems.textsecuregcm.registration.MessageTransport;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.UsernameNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;

@ExtendWith(DropwizardExtensionsSupport.class)
class AccountControllerTest {

  private static final String SENDER             = "+14152222222";
  private static final String SENDER_OLD         = "+14151111111";
  private static final String SENDER_PIN         = "+14153333333";
  private static final String SENDER_OVER_PIN    = "+14154444444";
  private static final String SENDER_OVER_PREFIX = "+14156666666";
  private static final String SENDER_PREAUTH     = "+14157777777";
  private static final String SENDER_REG_LOCK    = "+14158888888";
  private static final String SENDER_HAS_STORAGE = "+14159999999";
  private static final String SENDER_TRANSFER    = "+14151111112";
  private static final String RESTRICTED_COUNTRY = "800";
  private static final String RESTRICTED_NUMBER  = "+" + RESTRICTED_COUNTRY + "11111111";

  private static final UUID   SENDER_REG_LOCK_UUID = UUID.randomUUID();
  private static final UUID   SENDER_TRANSFER_UUID = UUID.randomUUID();
  private static final UUID   RESERVATION_TOKEN    = UUID.randomUUID();

  private static final String NICE_HOST                = "127.0.0.1";
  private static final String RATE_LIMITED_IP_HOST     = "10.0.0.1";
  private static final String RATE_LIMITED_PREFIX_HOST = "10.0.0.2";
  private static final String RATE_LIMITED_HOST2       = "10.0.0.3";

  private static final String VALID_CAPTCHA_TOKEN   = "valid_token";
  private static final String INVALID_CAPTCHA_TOKEN = "invalid_token";

  private static final String  TEST_NUMBER            = "+14151111113";

  private static StoredVerificationCodeManager pendingAccountsManager = mock(StoredVerificationCodeManager.class);
  private static AccountsManager accountsManager = mock(AccountsManager.class);
  private static RateLimiters rateLimiters = mock(RateLimiters.class);
  private static RateLimiter rateLimiter = mock(RateLimiter.class);
  private static RateLimiter pinLimiter = mock(RateLimiter.class);
  private static RateLimiter smsVoiceIpLimiter = mock(RateLimiter.class);
  private static RateLimiter smsVoicePrefixLimiter = mock(RateLimiter.class);
  private static RateLimiter autoBlockLimiter = mock(RateLimiter.class);
  private static RateLimiter usernameSetLimiter = mock(RateLimiter.class);
  private static RateLimiter usernameReserveLimiter = mock(RateLimiter.class);
  private static RateLimiter usernameLookupLimiter = mock(RateLimiter.class);
  private static RegistrationServiceClient registrationServiceClient = mock(RegistrationServiceClient.class);
  private static TurnTokenGenerator turnTokenGenerator = mock(TurnTokenGenerator.class);
  private static Account senderPinAccount = mock(Account.class);
  private static Account senderRegLockAccount = mock(Account.class);
  private static Account senderHasStorage = mock(Account.class);
  private static Account senderTransfer = mock(Account.class);
  private static CaptchaChecker captchaChecker = mock(CaptchaChecker.class);
  private static PushNotificationManager pushNotificationManager = mock(PushNotificationManager.class);
  private static ChangeNumberManager changeNumberManager = mock(ChangeNumberManager.class);
  private static ClientPresenceManager clientPresenceManager = mock(ClientPresenceManager.class);
  private static TestClock testClock = TestClock.now();

  private static DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

  private byte[] registration_lock_key = new byte[32];
  private static ExternalServiceCredentialGenerator storageCredentialGenerator = new ExternalServiceCredentialGenerator(new byte[32], new byte[32], false);

  private static final ResourceExtension resources = ResourceExtension.builder()
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
      .addResource(new AccountController(pendingAccountsManager,
          accountsManager,
          rateLimiters,
          registrationServiceClient,
          dynamicConfigurationManager,
          turnTokenGenerator,
          Map.of(TEST_NUMBER, 123456),
          captchaChecker,
          pushNotificationManager,
          changeNumberManager,
          storageCredentialGenerator,
          clientPresenceManager,
          testClock))
      .build();


  @BeforeEach
  void setup() throws Exception {
    clearInvocations(AuthHelper.VALID_ACCOUNT, AuthHelper.UNDISCOVERABLE_ACCOUNT);

    new SecureRandom().nextBytes(registration_lock_key);
    AuthenticationCredentials registrationLockCredentials = new AuthenticationCredentials(Hex.toStringCondensed(registration_lock_key));

    AccountsHelper.setupMockUpdate(accountsManager);

    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationDailyLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getPinLimiter()).thenReturn(pinLimiter);
    when(rateLimiters.getSmsVoiceIpLimiter()).thenReturn(smsVoiceIpLimiter);
    when(rateLimiters.getSmsVoicePrefixLimiter()).thenReturn(smsVoicePrefixLimiter);
    when(rateLimiters.getUsernameSetLimiter()).thenReturn(usernameSetLimiter);
    when(rateLimiters.getUsernameReserveLimiter()).thenReturn(usernameReserveLimiter);
    when(rateLimiters.getUsernameLookupLimiter()).thenReturn(usernameLookupLimiter);

    when(senderPinAccount.getLastSeen()).thenReturn(System.currentTimeMillis());
    when(senderPinAccount.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), System.currentTimeMillis()));

    when(senderHasStorage.getUuid()).thenReturn(UUID.randomUUID());
    when(senderHasStorage.isStorageSupported()).thenReturn(true);
    when(senderHasStorage.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), System.currentTimeMillis()));

    when(senderRegLockAccount.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.of(registrationLockCredentials.getHashedAuthenticationToken()), Optional.of(registrationLockCredentials.getSalt()), System.currentTimeMillis()));
    when(senderRegLockAccount.getLastSeen()).thenReturn(System.currentTimeMillis());
    when(senderRegLockAccount.getUuid()).thenReturn(SENDER_REG_LOCK_UUID);
    when(senderRegLockAccount.getNumber()).thenReturn(SENDER_REG_LOCK);

    when(senderTransfer.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), System.currentTimeMillis()));
    when(senderTransfer.getUuid()).thenReturn(SENDER_TRANSFER_UUID);
    when(senderTransfer.getNumber()).thenReturn(SENDER_TRANSFER);

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OLD)).thenReturn(Optional.empty());
    when(pendingAccountsManager.getCodeForNumber(SENDER_PIN)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PIN)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PREFIX)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_PREAUTH)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), "validchallenge", null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_HAS_STORAGE)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_TRANSFER)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), null, null)));

    when(accountsManager.getByE164(eq(SENDER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.getByE164(eq(SENDER_REG_LOCK))).thenReturn(Optional.of(senderRegLockAccount));
    when(accountsManager.getByE164(eq(SENDER_OVER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.getByE164(eq(SENDER))).thenReturn(Optional.empty());
    when(accountsManager.getByE164(eq(SENDER_OLD))).thenReturn(Optional.empty());
    when(accountsManager.getByE164(eq(SENDER_PREAUTH))).thenReturn(Optional.empty());
    when(accountsManager.getByE164(eq(SENDER_HAS_STORAGE))).thenReturn(Optional.of(senderHasStorage));
    when(accountsManager.getByE164(eq(SENDER_TRANSFER))).thenReturn(Optional.of(senderTransfer));

    when(accountsManager.create(any(), any(), any(), any(), any())).thenAnswer((Answer<Account>) invocation -> {
      final Account account = mock(Account.class);
      when(account.getUuid()).thenReturn(UUID.randomUUID());
      when(account.getNumber()).thenReturn(invocation.getArgument(0, String.class));
      when(account.getBadges()).thenReturn(invocation.getArgument(4, List.class));

      return account;
    });

    when(accountsManager.setUsername(AuthHelper.VALID_ACCOUNT, "takenusername", null))
        .thenThrow(new UsernameNotAvailableException());

    when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any())).thenAnswer((Answer<Account>) invocation -> {
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

    {
      DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
      when(dynamicConfigurationManager.getConfiguration())
          .thenReturn(dynamicConfiguration);

      DynamicCaptchaConfiguration signupCaptchaConfig = new DynamicCaptchaConfiguration();
      signupCaptchaConfig.setSignupCountryCodes(Set.of(RESTRICTED_COUNTRY));

      when(dynamicConfiguration.getCaptchaConfiguration()).thenReturn(signupCaptchaConfig);
    }
    when(captchaChecker.verify(eq(INVALID_CAPTCHA_TOKEN), anyString()))
        .thenReturn(AssessmentResult.invalid());
    when(captchaChecker.verify(eq(VALID_CAPTCHA_TOKEN), anyString()))
        .thenReturn(new AssessmentResult(true, ""));

    doThrow(new RateLimitExceededException(Duration.ZERO)).when(pinLimiter).validate(eq(SENDER_OVER_PIN));

    doThrow(new RateLimitExceededException(Duration.ZERO)).when(smsVoicePrefixLimiter).validate(SENDER_OVER_PREFIX.substring(0, 4+2));
    doThrow(new RateLimitExceededException(Duration.ZERO)).when(smsVoiceIpLimiter).validate(RATE_LIMITED_IP_HOST);
    doThrow(new RateLimitExceededException(Duration.ZERO)).when(smsVoiceIpLimiter).validate(RATE_LIMITED_HOST2);
  }

  @AfterEach
  void teardown() {
    reset(
        pendingAccountsManager,
        accountsManager,
        rateLimiters,
        rateLimiter,
        pinLimiter,
        smsVoiceIpLimiter,
        smsVoicePrefixLimiter,
        usernameSetLimiter,
        usernameReserveLimiter,
        usernameLookupLimiter,
        registrationServiceClient,
        turnTokenGenerator,
        senderPinAccount,
        senderRegLockAccount,
        senderHasStorage,
        senderTransfer,
        captchaChecker,
        pushNotificationManager,
        changeNumberManager,
        clientPresenceManager);

    clearInvocations(AuthHelper.DISABLED_DEVICE);
  }

  @Test
  void testGetFcmPreauth() throws NumberParseException {
    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new byte[16]));

    Response response = resources.getJerseyTest()
                                 .target("/v1/accounts/fcm/preauth/mytoken/+14152222222")
                                 .request()
                                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    final ArgumentCaptor<String> challengeTokenCaptor = ArgumentCaptor.forClass(String.class);

    verify(registrationServiceClient).createRegistrationSession(
        eq(PhoneNumberUtil.getInstance().parse("+14152222222", null)), any());

    verify(pushNotificationManager).sendRegistrationChallengeNotification(
        eq("mytoken"), eq(PushNotification.TokenType.FCM), challengeTokenCaptor.capture());

    assertThat(challengeTokenCaptor.getValue().length()).isEqualTo(32);
  }

  @Test
  void testGetFcmPreauthIvoryCoast() throws NumberParseException {
    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new byte[16]));

    Response response = resources.getJerseyTest()
            .target("/v1/accounts/fcm/preauth/mytoken/+2250707312345")
            .request()
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    final ArgumentCaptor<String> challengeTokenCaptor = ArgumentCaptor.forClass(String.class);

    verify(registrationServiceClient).createRegistrationSession(
        eq(PhoneNumberUtil.getInstance().parse("+2250707312345", null)), any());

    verify(pushNotificationManager).sendRegistrationChallengeNotification(
        eq("mytoken"), eq(PushNotification.TokenType.FCM), challengeTokenCaptor.capture());

    assertThat(challengeTokenCaptor.getValue().length()).isEqualTo(32);
  }

  @Test
  void testGetApnPreauth() throws NumberParseException {
    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new byte[16]));

    Response response = resources.getJerseyTest()
                                 .target("/v1/accounts/apn/preauth/mytoken/+14152222222")
                                 .request()
                                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    final ArgumentCaptor<String> challengeTokenCaptor = ArgumentCaptor.forClass(String.class);

    verify(registrationServiceClient).createRegistrationSession(
        eq(PhoneNumberUtil.getInstance().parse("+14152222222", null)), any());

    verify(pushNotificationManager).sendRegistrationChallengeNotification(
        eq("mytoken"), eq(PushNotification.TokenType.APN_VOIP), challengeTokenCaptor.capture());

    assertThat(challengeTokenCaptor.getValue().length()).isEqualTo(32);
  }

  @Test
  void testGetApnPreauthExplicitVoip() throws NumberParseException {
    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new byte[16]));

    Response response = resources.getJerseyTest()
        .target("/v1/accounts/apn/preauth/mytoken/+14152222222")
        .queryParam("voip", "true")
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(200);

    final ArgumentCaptor<String> challengeTokenCaptor = ArgumentCaptor.forClass(String.class);

    verify(registrationServiceClient).createRegistrationSession(
        eq(PhoneNumberUtil.getInstance().parse("+14152222222", null)), any());

    verify(pushNotificationManager).sendRegistrationChallengeNotification(
        eq("mytoken"), eq(PushNotification.TokenType.APN_VOIP), challengeTokenCaptor.capture());

    assertThat(challengeTokenCaptor.getValue().length()).isEqualTo(32);
  }

  @Test
  void testGetApnPreauthExplicitNoVoip() throws NumberParseException {
    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new byte[16]));

    Response response = resources.getJerseyTest()
        .target("/v1/accounts/apn/preauth/mytoken/+14152222222")
        .queryParam("voip", "false")
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(200);

    final ArgumentCaptor<String> challengeTokenCaptor = ArgumentCaptor.forClass(String.class);

    verify(registrationServiceClient).createRegistrationSession(
        eq(PhoneNumberUtil.getInstance().parse("+14152222222", null)), any());

    verify(pushNotificationManager).sendRegistrationChallengeNotification(
        eq("mytoken"), eq(PushNotification.TokenType.APN), challengeTokenCaptor.capture());

    assertThat(challengeTokenCaptor.getValue().length()).isEqualTo(32);
  }

  @Test
  void testGetPreauthImpossibleNumber() {
    final Response response = resources.getJerseyTest()
        .target("/v1/accounts/fcm/preauth/mytoken/BogusNumber")
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isBlank();

    verifyNoInteractions(registrationServiceClient);
    verifyNoInteractions(pushNotificationManager);
  }

  @Test
  void testGetPreauthNonNormalized() {
    final String number = "+4407700900111";

    final Response response = resources.getJerseyTest()
        .target("/v1/accounts/fcm/preauth/mytoken/" + number)
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(400);

    final NonNormalizedPhoneNumberResponse responseEntity = response.readEntity(NonNormalizedPhoneNumberResponse.class);
    assertThat(responseEntity.getOriginalNumber()).isEqualTo(number);
    assertThat(responseEntity.getNormalizedNumber()).isEqualTo("+447700900111");

    verifyNoInteractions(registrationServiceClient);
    verifyNoInteractions(pushNotificationManager);
  }

  @Test
  void testSendCodeWithExistingSessionFromPreauth() {
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER))
        .thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", sessionId)));

    when(registrationServiceClient.sendRegistrationCode(eq(sessionId), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
    verify(pendingAccountsManager).store(eq(SENDER), argThat(
        storedVerificationCode -> Arrays.equals(storedVerificationCode.sessionId(), sessionId) &&
            "1234-push".equals(storedVerificationCode.pushCode())));
  }

  @Test
  void testSendCode() {
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
    verify(pendingAccountsManager).store(eq(SENDER), argThat(
        storedVerificationCode -> Arrays.equals(storedVerificationCode.sessionId(), sessionId) &&
            "1234-push".equals(storedVerificationCode.pushCode())));
  }

  @Test
  void testSendCodeRateLimited() {
    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(Duration.ofMinutes(10))));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(413);

    verify(registrationServiceClient, never()).sendRegistrationCode(any(), any(), any(), any(), any());
  }

  @Test
  void testSendCodeImpossibleNumber() {
    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", "Definitely not a real number"))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isBlank();

    verify(registrationServiceClient, never()).sendRegistrationCode(any(), any(), any(), any(), any());
  }

  @Test
  void testSendCodeNonNormalized() {
    final String number = "+4407700900111";

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", number))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(400);

    final NonNormalizedPhoneNumberResponse responseEntity = response.readEntity(NonNormalizedPhoneNumberResponse.class);
    assertThat(responseEntity.getOriginalNumber()).isEqualTo(number);
    assertThat(responseEntity.getNormalizedNumber()).isEqualTo("+447700900111");

    verify(registrationServiceClient, never()).sendRegistrationCode(any(), any(), any(), any(), any());
  }

  @Test
  public void testSendCodeVoiceNoLocale() {

    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/voice/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(200);
    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.VOICE, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testSendCodeWithValidPreauth() {

    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_PREAUTH))
                 .queryParam("challenge", "validchallenge")
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testSendCodeWithInvalidPreauth() {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_PREAUTH))
                 .queryParam("challenge", "invalidchallenge")
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoInteractions(registrationServiceClient);
  }

  @Test
  void testSendCodeWithNoPreauth() {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_PREAUTH))
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verifyNoInteractions(registrationServiceClient);
  }

  @Test
  void testSendiOSCode() {

    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("client", "ios")
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.IOS, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testSendAndroidNgCode() {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("client", "android-ng")
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.ANDROID_WITHOUT_FCM, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testSendWithValidCaptcha() throws NumberParseException, IOException {

    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("captcha", VALID_CAPTCHA_TOKEN)
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(captchaChecker).verify(eq(VALID_CAPTCHA_TOKEN), eq(NICE_HOST));
    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testSendWithInvalidCaptcha() throws IOException {

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("captcha", INVALID_CAPTCHA_TOKEN)
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(captchaChecker).verify(eq(INVALID_CAPTCHA_TOKEN), eq(NICE_HOST));
    verifyNoInteractions(registrationServiceClient);
  }

  @Test
  void testSendRateLimitedHost() {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, RATE_LIMITED_IP_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verifyNoInteractions(captchaChecker);
    verifyNoInteractions(registrationServiceClient);
  }

  @Test
  void testSendRateLimitedPrefixAutoBlock() {

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_OVER_PREFIX))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, RATE_LIMITED_PREFIX_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verifyNoInteractions(captchaChecker);
    verifyNoInteractions(registrationServiceClient);
  }

  @Test
  void testSendRestrictedHostOut() {

    final String challenge = "challenge";
    when(pendingAccountsManager.getCodeForNumber(RESTRICTED_NUMBER))
        .thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), challenge, null)));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", RESTRICTED_NUMBER))
                 .queryParam("challenge", challenge)
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verifyNoInteractions(registrationServiceClient);
  }

  @ParameterizedTest
  @CsvSource({
      "+12025550123, true",
      "+12505550199, false",
  })
  void testRestrictedRegion(final String number, final boolean expectSendCode) throws NumberParseException {
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    final DynamicCaptchaConfiguration signupCaptchaConfig = new DynamicCaptchaConfiguration();
    signupCaptchaConfig.setSignupRegions(Set.of("CA"));

    when(dynamicConfiguration.getCaptchaConfiguration()).thenReturn(signupCaptchaConfig);

    final String challenge = "challenge";
    when(pendingAccountsManager.getCodeForNumber(number))
        .thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), challenge, null)));

    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", number))
            .queryParam("challenge", challenge)
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    if (expectSendCode) {
      assertThat(response.getStatus()).isEqualTo(200);
      verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
    } else {
      assertThat(response.getStatus()).isEqualTo(402);
      verifyNoInteractions(registrationServiceClient);
    }
  }

  @Test
  void testSendRestrictedIn() {

    final String challenge = "challenge";
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of(new StoredVerificationCode(null, System.currentTimeMillis(), challenge, null)));
    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", challenge)
                 .request()
                 .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testSendCodeTestDeviceNumber() {
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", TEST_NUMBER))
            .request()
            .header("X-Forwarded-For", RATE_LIMITED_IP_HOST)
            .get();

    final ArgumentCaptor<StoredVerificationCode> captor = ArgumentCaptor.forClass(StoredVerificationCode.class);
    verify(pendingAccountsManager).store(eq(TEST_NUMBER), captor.capture());
    assertThat(captor.getValue().code()).isNull();
    assertThat(captor.getValue().sessionId()).isEqualTo(sessionId);
    assertThat(response.getStatus()).isEqualTo(200);

    // Even though no actual SMS will be sent, we leave that decision to the registration service
    verify(registrationServiceClient).sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testVerifyCode() throws Exception {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "1234", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    resources.getJerseyTest()
        .target("/v1/accounts/code/1234")
        .request()
        .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER, "bar"))
        .put(Entity.entity(new AccountAttributes(), MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(accountsManager).create(eq(SENDER), eq("bar"), any(), any(), anyList());

    verify(registrationServiceClient).checkVerificationCode(sessionId, "1234", AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  @Test
  void testVerifyCodeBadCredentials() {
    final Response response = resources.getJerseyTest()
        .target("/v1/accounts/code/1234")
        .request()
        .header("Authorization", "This is not a valid authorization header")
        .put(Entity.entity(new AccountAttributes(), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testVerifyCodeOld() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/1234")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_OLD, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoInteractions(accountsManager);
  }

  @Test
  void testVerifyBadCode() {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(false));

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/1111")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verify(registrationServiceClient).checkVerificationCode(sessionId, "1111", AccountController.REGISTRATION_RPC_TIMEOUT);
    verifyNoInteractions(accountsManager);
  }

  @Test
  void testVerifyRegistrationLock() throws Exception {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "666666-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "666666", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    AccountIdentityResponse result =
        resources.getJerseyTest()
            .target("/v1/accounts/code/666666")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, Hex.toStringCondensed(registration_lock_key), true, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    assertThat(result.uuid()).isNotNull();

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));
  }

  @Test
  void testVerifyRegistrationLockSetsRegistrationLockOnNewAccount() throws Exception {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "666666-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "666666", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    AccountIdentityResponse result =
        resources.getJerseyTest()
            .target("/v1/accounts/code/666666")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, Hex.toStringCondensed(registration_lock_key), true, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    assertThat(result.uuid()).isNotNull();

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));

    verify(accountsManager).create(eq(SENDER_REG_LOCK), eq("bar"), any(), argThat(
        attributes -> Hex.toStringCondensed(registration_lock_key).equals(attributes.getRegistrationLock())),
        argThat(List::isEmpty));
  }

  @Test
  void testVerifyRegistrationLockOld() {
    StoredRegistrationLock lock = senderRegLockAccount.getRegistrationLock();

    try {
      when(senderRegLockAccount.getRegistrationLock()).thenReturn(lock.forTime(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7)));

      final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

      when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK))
          .thenReturn(Optional.of(
              new StoredVerificationCode(null, System.currentTimeMillis(), "666666-push", sessionId)));

      when(registrationServiceClient.checkVerificationCode(sessionId, "666666", AccountController.REGISTRATION_RPC_TIMEOUT))
          .thenReturn(CompletableFuture.completedFuture(true));

      AccountIdentityResponse result =
          resources.getJerseyTest()
              .target("/v1/accounts/code/666666")
              .request()
              .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
              .put(Entity.entity(new AccountAttributes(false, 3333, null, null, true, null),
                  MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

      assertThat(result.uuid()).isNotNull();

      verifyNoInteractions(pinLimiter);
    } finally {
      when(senderRegLockAccount.getRegistrationLock()).thenReturn(lock);
    }
  }

  @Test
  void testVerifyWrongRegistrationLock() throws Exception {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "666666-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "666666", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/666666")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null,
                    Hex.toStringCondensed(new byte[32]), true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    // verify(senderRegLockAccount).lockAuthenticationCredentials();
    // verify(clientPresenceManager, times(1)).disconnectAllPresences(eq(SENDER_REG_LOCK_UUID), any());
    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));
  }

  @Test
  void testVerifyNoRegistrationLock() {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "666666-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "666666", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/666666")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    RegistrationLockFailure failure = response.readEntity(RegistrationLockFailure.class);
    assertThat(failure.backupCredentials()).isNotNull();
    assertThat(failure.backupCredentials().username()).isEqualTo(SENDER_REG_LOCK_UUID.toString());
    assertThat(failure.backupCredentials().password()).isNotEmpty();
    assertThat(failure.backupCredentials().password().startsWith(SENDER_REG_LOCK_UUID.toString())).isTrue();
    assertThat(failure.timeRemaining()).isGreaterThan(0);

    // verify(senderRegLockAccount).lockAuthenticationCredentials();
    // verify(clientPresenceManager, atLeastOnce()).disconnectAllPresences(eq(SENDER_REG_LOCK_UUID), any());
    verifyNoInteractions(pinLimiter);
  }

  @Test
  void testVerifyTransferSupported() {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_TRANSFER))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "1234", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    when(senderTransfer.isTransferSupported()).thenReturn(true);

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/1234")
            .queryParam("transfer", true)
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_TRANSFER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testVerifyTransferNotSupported() {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_TRANSFER))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "1234", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    when(senderTransfer.isTransferSupported()).thenReturn(false);

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/1234")
            .queryParam("transfer", true)
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_TRANSFER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testVerifyTransferSupportedNotRequested() {
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(SENDER_TRANSFER))
        .thenReturn(Optional.of(
            new StoredVerificationCode(null, System.currentTimeMillis(), "1234-push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(sessionId, "1234", AccountController.REGISTRATION_RPC_TIMEOUT))
        .thenReturn(CompletableFuture.completedFuture(true));

    when(senderTransfer.isTransferSupported()).thenReturn(true);

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/code/1234")
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_TRANSFER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testChangePhoneNumber() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(null, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final AccountIdentityResponse accountIdentityResponse =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(registrationServiceClient).checkVerificationCode(sessionId, code, AccountController.REGISTRATION_RPC_TIMEOUT);

    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(number), any(), any(), any(), any());

    assertThat(accountIdentityResponse.uuid()).isEqualTo(AuthHelper.VALID_UUID);
    assertThat(accountIdentityResponse.number()).isEqualTo(number);
    assertThat(accountIdentityResponse.pni()).isNotEqualTo(AuthHelper.VALID_PNI);
  }

  @Test
  void testChangePhoneNumberImpossibleNumber() throws Exception {
    final String number = "This is not a real phone number";
    final String code = "987654";

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isBlank();
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberNonNormalized() throws Exception {
    final String number = "+4407700900111";
    final String code = "987654";

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);

    final NonNormalizedPhoneNumberResponse responseEntity = response.readEntity(NonNormalizedPhoneNumberResponse.class);
    assertThat(responseEntity.getOriginalNumber()).isEqualTo(number);
    assertThat(responseEntity.getNormalizedNumber()).isEqualTo("+447700900111");

    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberSameNumber() throws Exception {
    final AccountIdentityResponse accountIdentityResponse =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(AuthHelper.VALID_NUMBER, "567890", null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberNoPendingCode() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.empty());

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberIncorrectCode() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(false));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    verify(registrationServiceClient).checkVerificationCode(sessionId, code, AccountController.REGISTRATION_RPC_TIMEOUT);

    assertThat(response.getStatus()).isEqualTo(403);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockNotRequired() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final StoredRegistrationLock existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(false);

    final Account existingAccount = mock(Account.class);
    when(existingAccount.getNumber()).thenReturn(number);
    when(existingAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(existingAccount.getRegistrationLock()).thenReturn(existingRegistrationLock);

    when(accountsManager.getByE164(number)).thenReturn(Optional.of(existingAccount));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockRequiredNotProvided() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final StoredRegistrationLock existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(true);

    final UUID existingUuid = UUID.randomUUID();
    final Account existingAccount = mock(Account.class);
    when(existingAccount.getNumber()).thenReturn(number);
    when(existingAccount.getUuid()).thenReturn(existingUuid);
    when(existingAccount.getRegistrationLock()).thenReturn(existingRegistrationLock);

    when(accountsManager.getByE164(number)).thenReturn(Optional.of(existingAccount));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    // verify(existingAccount).lockAuthenticationCredentials();
    // verify(clientPresenceManager, atLeastOnce()).disconnectAllPresences(eq(existingUuid), any());
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockRequiredIncorrect() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final String reglock = "setec-astronomy";
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(null, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final StoredRegistrationLock existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(true);
    when(existingRegistrationLock.verify(anyString())).thenReturn(false);

    UUID existingUuid = UUID.randomUUID();
    final Account existingAccount = mock(Account.class);
    when(existingAccount.getNumber()).thenReturn(number);
    when(existingAccount.getUuid()).thenReturn(existingUuid);
    when(existingAccount.getRegistrationLock()).thenReturn(existingRegistrationLock);

    when(accountsManager.getByE164(number)).thenReturn(Optional.of(existingAccount));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, reglock, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    // verify(existingAccount).lockAuthenticationCredentials();
    // verify(clientPresenceManager, atLeastOnce()).disconnectAllPresences(eq(existingUuid), any());
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockRequiredCorrect() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final String reglock = "setec-astronomy";
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(null, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final StoredRegistrationLock existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(true);
    when(existingRegistrationLock.verify(reglock)).thenReturn(true);

    final Account existingAccount = mock(Account.class);
    when(existingAccount.getNumber()).thenReturn(number);
    when(existingAccount.getUuid()).thenReturn(UUID.randomUUID());
    when(existingAccount.getRegistrationLock()).thenReturn(existingRegistrationLock);

    when(accountsManager.getByE164(number)).thenReturn(Optional.of(existingAccount));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, reglock, null, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    verify(senderRegLockAccount, never()).lockAuthenticationCredentials();
    verify(clientPresenceManager, never()).disconnectAllPresences(eq(SENDER_REG_LOCK_UUID), any());
    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any(), any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberChangePrekeys() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final String pniIdentityKey = "changed-pni-identity-key";
    final byte[] sessionId = "session-id".getBytes(StandardCharsets.UTF_8);

    Device device2 = mock(Device.class);
    when(device2.getId()).thenReturn(2L);
    when(device2.isEnabled()).thenReturn(true);
    when(device2.getRegistrationId()).thenReturn(2);

    Device device3 = mock(Device.class);
    when(device3.getId()).thenReturn(3L);
    when(device3.isEnabled()).thenReturn(true);
    when(device3.getRegistrationId()).thenReturn(3);

    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(List.of(AuthHelper.VALID_DEVICE, device2, device3));
    when(AuthHelper.VALID_ACCOUNT.getDevice(2L)).thenReturn(Optional.of(device2));
    when(AuthHelper.VALID_ACCOUNT.getDevice(3L)).thenReturn(Optional.of(device3));

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(null, System.currentTimeMillis(), "push", sessionId)));

    when(registrationServiceClient.checkVerificationCode(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    var deviceMessages = List.of(
            new IncomingMessage(1, 2, 2, "content2"),
            new IncomingMessage(1, 3, 3, "content3"));
    var deviceKeys = Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey(), 3L, new SignedPreKey());

    final Map<Long, Integer> registrationIds = Map.of(1L, 17, 2L, 47, 3L, 89);

    final AccountIdentityResponse accountIdentityResponse =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(
                    number, code, null,
                    pniIdentityKey, deviceMessages,
                    deviceKeys,
                    registrationIds),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(number), any(), any(), any(), any());

    assertThat(accountIdentityResponse.uuid()).isEqualTo(AuthHelper.VALID_UUID);
    assertThat(accountIdentityResponse.number()).isEqualTo(number);
    assertThat(accountIdentityResponse.pni()).isNotEqualTo(AuthHelper.VALID_PNI);
  }

  @Test
  void testSetRegistrationLock() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("1234567890123456789012345678901234567890123456789012345678901234")));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<String> pinCapture     = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> pinSaltCapture = ArgumentCaptor.forClass(String.class);

    verify(AuthHelper.VALID_ACCOUNT, times(1)).setRegistrationLock(pinCapture.capture(), pinSaltCapture.capture());

    assertThat(pinCapture.getValue()).isNotEmpty();
    assertThat(pinSaltCapture.getValue()).isNotEmpty();

    assertThat(pinCapture.getValue().length()).isEqualTo(66);
  }

  @Test
  void testSetShortRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("313")));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testSetRegistrationLockDisabled() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new RegistrationLock("1234567890123456789012345678901234567890123456789012345678901234")));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetGcmId() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/gcm/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new GcmRegistrationId("z000")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setGcmId(eq("z000"));
    verify(accountsManager, times(1)).updateDevice(eq(AuthHelper.DISABLED_ACCOUNT), anyLong(), any());
  }

  @Test
  void testSetApnId() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/apn/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new ApnRegistrationId("first", "second")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setApnId(eq("first"));
    verify(AuthHelper.DISABLED_DEVICE, times(1)).setVoipApnId(eq("second"));
    verify(accountsManager, times(1)).updateDevice(eq(AuthHelper.DISABLED_ACCOUNT), anyLong(), any());
  }

  @Test
  void testSetApnIdNoVoip() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/apn/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID, AuthHelper.DISABLED_PASSWORD))
            .put(Entity.json(new ApnRegistrationId("first", null)));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setApnId(eq("first"));
    verify(AuthHelper.DISABLED_DEVICE, times(1)).setVoipApnId(null);
    verify(accountsManager, times(1)).updateDevice(eq(AuthHelper.DISABLED_ACCOUNT), anyLong(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/v1/accounts/whoami/", "/v1/accounts/me/"})
  public void testWhoAmI(final String path) {
    AccountIdentityResponse response =
        resources.getJerseyTest()
                 .target(path)
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .get(AccountIdentityResponse.class);

    assertThat(response.uuid()).isEqualTo(AuthHelper.VALID_UUID);
  }

  @Test
  void testSetUsername() throws UsernameNotAvailableException {
    Account account = mock(Account.class);
    when(account.getUsername()).thenReturn(Optional.of("N00bkilleR.1234"));
    when(accountsManager.setUsername(any(), eq("N00bkilleR"), isNull()))
        .thenReturn(account);
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new UsernameRequest("N00bkilleR", null)));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(UsernameResponse.class).username()).isEqualTo("N00bkilleR.1234");
  }

  @Test
  void testReserveUsername() throws UsernameNotAvailableException {
    when(accountsManager.reserveUsername(any(), eq("N00bkilleR")))
        .thenReturn(new AccountsManager.UsernameReservation(null, "N00bkilleR.1234", RESERVATION_TOKEN));
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username/reserved")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ReserveUsernameRequest("N00bkilleR")));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(ReserveUsernameResponse.class))
        .satisfies(r -> r.username().equals("N00bkilleR.1234"))
        .satisfies(r -> r.reservationToken().equals(RESERVATION_TOKEN));
  }

  @Test
  void testCommitUsername() throws UsernameNotAvailableException, UsernameReservationNotFoundException {
    Account account = mock(Account.class);
    when(account.getUsername()).thenReturn(Optional.of("n00bkiller.1234"));
    when(accountsManager.confirmReservedUsername(any(), eq("n00bkiller.1234"), eq(RESERVATION_TOKEN))).thenReturn(account);
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username/confirm")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameRequest("n00bkiller.1234", RESERVATION_TOKEN)));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(UsernameResponse.class).username()).isEqualTo("n00bkiller.1234");
  }

  @Test
  void testCommitUnreservedUsername() throws UsernameNotAvailableException, UsernameReservationNotFoundException {
    when(accountsManager.confirmReservedUsername(any(), eq("n00bkiller.1234"), eq(RESERVATION_TOKEN)))
        .thenThrow(new UsernameReservationNotFoundException());
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username/confirm")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameRequest("n00bkiller.1234", RESERVATION_TOKEN)));
    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testCommitLapsedUsername() throws UsernameNotAvailableException, UsernameReservationNotFoundException {
    when(accountsManager.confirmReservedUsername(any(), eq("n00bkiller.1234"), eq(RESERVATION_TOKEN)))
        .thenThrow(new UsernameNotAvailableException());
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username/confirm")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameRequest("n00bkiller.1234", RESERVATION_TOKEN)));
    assertThat(response.getStatus()).isEqualTo(410);
  }

  @Test
  void testSetTakenUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new UsernameRequest("takenusername", null)));

    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testSetInvalidUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 // contains non-ascii character
                 .put(Entity.json(new UsernameRequest("pаypal", null)));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testSetInvalidPrefixUsername() throws JsonProcessingException {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new UsernameRequest("0n00bkiller", null)));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testSetUsernameBadAuth() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
                 .put(Entity.json(new UsernameRequest("n00bkiller", null)));
    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testDeleteUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .delete();

    assertThat(response.getStatus()).isEqualTo(204);
    verify(accountsManager).clearUsername(AuthHelper.VALID_ACCOUNT);
  }

  @Test
  void testDeleteUsernameBadAuth() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
                 .delete();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetAccountAttributesNoDiscoverabilityChange() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, null, null, true, null)));

    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  void testSetAccountAttributesEnableDiscovery() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.UNDISCOVERABLE_UUID, AuthHelper.UNDISCOVERABLE_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, null, null, true, null)));

    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  void testSetAccountAttributesDisableDiscovery() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, null, null, false, null)));

    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  void testSetAccountAttributesBadUnidentifiedKeyLength() {
    final AccountAttributes attributes = new AccountAttributes(false, 2222, null, null, false, null);
    attributes.setUnidentifiedAccessKey(new byte[7]);

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/attributes/")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(attributes));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testDeleteAccount() throws InterruptedException {
    Response response =
            resources.getJerseyTest()
                     .target("/v1/accounts/me")
                     .request()
                     .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                     .delete();

    assertThat(response.getStatus()).isEqualTo(204);
    verify(accountsManager).delete(AuthHelper.VALID_ACCOUNT, AccountsManager.DeletionReason.USER_REQUEST);
  }

  @Test
  void testDeleteAccountInterrupted() throws InterruptedException {
    doThrow(InterruptedException.class).when(accountsManager).delete(any(), any());

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/me")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .delete();

    assertThat(response.getStatus()).isEqualTo(500);
    verify(accountsManager).delete(AuthHelper.VALID_ACCOUNT, AccountsManager.DeletionReason.USER_REQUEST);
  }

  @ParameterizedTest
  @MethodSource
  void testSignupCaptcha(final String message, final boolean enforced, final Set<String> countryCodes, final int expectedResponseStatusCode) {
    DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration())
        .thenReturn(dynamicConfiguration);

    DynamicCaptchaConfiguration signupCaptchaConfig = new DynamicCaptchaConfiguration();
    signupCaptchaConfig.setSignupCountryCodes(countryCodes);
    when(dynamicConfiguration.getCaptchaConfiguration())
        .thenReturn(signupCaptchaConfig);

    final byte[] sessionId = "session".getBytes(StandardCharsets.UTF_8);

    when(registrationServiceClient.sendRegistrationCode(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    when(registrationServiceClient.createRegistrationSession(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(sessionId));

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header(HttpHeaders.X_FORWARDED_FOR, NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(expectedResponseStatusCode);

    verify(registrationServiceClient, 200 == expectedResponseStatusCode ? times(1) : never())
        .sendRegistrationCode(sessionId, MessageTransport.SMS, ClientType.UNKNOWN, null, AccountController.REGISTRATION_RPC_TIMEOUT);
  }

  static Stream<Arguments> testSignupCaptcha() {
    return Stream.of(
        Arguments.of("captcha not enforced", false, Collections.emptySet(), 200),
        Arguments.of("no enforced country codes", true, Collections.emptySet(), 200),
        Arguments.of("captcha enforced", true, Set.of("1"), 402)
    );
  }

  @Test
  void testAccountExists() {
    final Account account = mock(Account.class);

    final UUID accountIdentifier = UUID.randomUUID();
    final UUID phoneNumberIdentifier = UUID.randomUUID();

    when(accountsManager.getByAccountIdentifier(any())).thenReturn(Optional.empty());
    when(accountsManager.getByAccountIdentifier(accountIdentifier)).thenReturn(Optional.of(account));
    when(accountsManager.getByPhoneNumberIdentifier(any())).thenReturn(Optional.empty());
    when(accountsManager.getByPhoneNumberIdentifier(phoneNumberIdentifier)).thenReturn(Optional.of(account));

    when(rateLimiters.getCheckAccountExistenceLimiter()).thenReturn(mock(RateLimiter.class));

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", accountIdentifier))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(200);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", phoneNumberIdentifier))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(200);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(404);
  }

  @Test
  void testAccountExistsRateLimited() throws RateLimitExceededException {
    final Account account = mock(Account.class);
    final UUID accountIdentifier = UUID.randomUUID();
    when(accountsManager.getByAccountIdentifier(accountIdentifier)).thenReturn(Optional.of(account));

    final RateLimiter checkAccountLimiter = mock(RateLimiter.class);
    when(rateLimiters.getCheckAccountExistenceLimiter()).thenReturn(checkAccountLimiter);
    doThrow(new RateLimitExceededException(Duration.ofSeconds(13))).when(checkAccountLimiter).validate("127.0.0.1");

    final Response response = resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", accountIdentifier))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .head();

    assertThat(response.getStatus()).isEqualTo(413);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(Duration.ofSeconds(13).toSeconds()));
  }

  @Test
  void testAccountExistsNoForwardedFor() throws RateLimitExceededException {
    final Response response = resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "")
        .head();

    assertThat(response.getStatus()).isEqualTo(413);
    assertThat(Long.parseLong(response.getHeaderString("Retry-After"))).isNotNegative();
  }

  @Test
  void testAccountExistsAuthenticated() {
    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(400);
  }

  @Test
  void testLookupUsername() {
    final Account account = mock(Account.class);
    final UUID uuid = UUID.randomUUID();
    when(account.getUuid()).thenReturn(uuid);

    when(accountsManager.getByUsername(eq("n00bkiller.1234"))).thenReturn(Optional.of(account));
    Response response = resources.getJerseyTest()
        .target("v1/accounts/username/n00bkiller.1234")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .get();
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(AccountIdentifierResponse.class).uuid()).isEqualTo(uuid);
  }

  @Test
  void testLookupUsernameDoesNotExist() {
    when(accountsManager.getByUsername(eq("n00bkiller.1234"))).thenReturn(Optional.empty());
    assertThat(resources.getJerseyTest()
        .target("v1/accounts/username/n00bkiller.1234")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .get().getStatus()).isEqualTo(404);
  }

  @Test
  void testLookupUsernameRateLimited() throws RateLimitExceededException {
    doThrow(new RateLimitExceededException(Duration.ofSeconds(13))).when(usernameLookupLimiter).validate("127.0.0.1");
    final Response response = resources.getJerseyTest()
        .target("/v1/accounts/username/test.123")
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, "127.0.0.1")
        .get();

    assertThat(response.getStatus()).isEqualTo(413);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(Duration.ofSeconds(13).toSeconds()));
  }

  @ParameterizedTest
  @MethodSource
  void pushTokensMatch(@Nullable final String pushChallenge, @Nullable final StoredVerificationCode storedVerificationCode, final boolean expectMatch) {
    final String number = "+18005550123";
    final Optional<String> maybePushChallenge = Optional.ofNullable(pushChallenge);
    final Optional<StoredVerificationCode> maybeStoredVerificationCode = Optional.ofNullable(storedVerificationCode);

    assertEquals(expectMatch, AccountController.pushChallengeMatches(number, maybePushChallenge, maybeStoredVerificationCode));
  }

  private static Stream<Arguments> pushTokensMatch() {
    return Stream.of(
        Arguments.of(null, null, false),
        Arguments.of("123456", null, false),
        Arguments.of(null, new StoredVerificationCode(null, 0, null, null), false),
        Arguments.of(null, new StoredVerificationCode(null, 0, "123456", null), false),
        Arguments.of("654321", new StoredVerificationCode(null, 0, "123456", null), false),
        Arguments.of("123456", new StoredVerificationCode(null, 0, "123456", null), true)
    );
  }
}
