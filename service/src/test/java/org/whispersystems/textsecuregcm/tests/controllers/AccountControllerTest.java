/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ChangePhoneNumberRequest;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberResponse;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.GcmMessage;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioVerifyExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.UsernameNotAvailableException;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.SystemMapper;

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

  private static final UUID   SENDER_REG_LOCK_UUID = UUID.randomUUID();
  private static final UUID   SENDER_TRANSFER_UUID = UUID.randomUUID();

  private static final String ABUSIVE_HOST             = "192.168.1.1";
  private static final String RESTRICTED_HOST          = "192.168.1.2";
  private static final String NICE_HOST                = "127.0.0.1";
  private static final String RATE_LIMITED_IP_HOST     = "10.0.0.1";
  private static final String RATE_LIMITED_PREFIX_HOST = "10.0.0.2";
  private static final String RATE_LIMITED_HOST2       = "10.0.0.3";

  private static final String VALID_CAPTCHA_TOKEN   = "valid_token";
  private static final String INVALID_CAPTCHA_TOKEN = "invalid_token";

  private static StoredVerificationCodeManager pendingAccountsManager = mock(StoredVerificationCodeManager.class);
  private static AccountsManager        accountsManager        = mock(AccountsManager.class);
  private static AbusiveHostRules       abusiveHostRules       = mock(AbusiveHostRules.class);
  private static RateLimiters           rateLimiters           = mock(RateLimiters.class);
  private static RateLimiter            rateLimiter            = mock(RateLimiter.class);
  private static RateLimiter            pinLimiter             = mock(RateLimiter.class);
  private static RateLimiter            smsVoiceIpLimiter      = mock(RateLimiter.class);
  private static RateLimiter            smsVoicePrefixLimiter  = mock(RateLimiter.class);
  private static RateLimiter            autoBlockLimiter       = mock(RateLimiter.class);
  private static RateLimiter            usernameSetLimiter     = mock(RateLimiter.class);
  private static SmsSender              smsSender              = mock(SmsSender.class);
  private static TurnTokenGenerator     turnTokenGenerator     = mock(TurnTokenGenerator.class);
  private static Account                senderPinAccount       = mock(Account.class);
  private static Account                senderRegLockAccount   = mock(Account.class);
  private static Account                senderHasStorage       = mock(Account.class);
  private static Account                senderTransfer         = mock(Account.class);
  private static RecaptchaClient        recaptchaClient        = mock(RecaptchaClient.class);
  private static GCMSender              gcmSender              = mock(GCMSender.class);
  private static APNSender              apnSender              = mock(APNSender.class);
  private static ChangeNumberManager    changeNumberManager    = mock(ChangeNumberManager.class);

  private static DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

  private static TwilioVerifyExperimentEnrollmentManager verifyExperimentEnrollmentManager = mock(
      TwilioVerifyExperimentEnrollmentManager.class);

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
          abusiveHostRules,
          rateLimiters,
          smsSender,
          dynamicConfigurationManager,
          turnTokenGenerator,
          new HashMap<>(),
          recaptchaClient,
          gcmSender,
          apnSender,
          verifyExperimentEnrollmentManager,
          changeNumberManager,
          storageCredentialGenerator))
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
    when(rateLimiters.getAutoBlockLimiter()).thenReturn(autoBlockLimiter);
    when(rateLimiters.getUsernameSetLimiter()).thenReturn(usernameSetLimiter);

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

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis(), "1234-push", null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OLD)).thenReturn(Optional.empty());
    when(pendingAccountsManager.getCodeForNumber(SENDER_PIN)).thenReturn(Optional.of(new StoredVerificationCode("333333", System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK)).thenReturn(Optional.of(new StoredVerificationCode("666666", System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PIN)).thenReturn(Optional.of(new StoredVerificationCode("444444", System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PREFIX)).thenReturn(Optional.of(new StoredVerificationCode("777777", System.currentTimeMillis(), "1234-push", null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_PREAUTH)).thenReturn(Optional.of(new StoredVerificationCode("555555", System.currentTimeMillis(), "validchallenge", null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_HAS_STORAGE)).thenReturn(Optional.of(new StoredVerificationCode("666666", System.currentTimeMillis(), null, null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_TRANSFER)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis(), null, null)));

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

    when(accountsManager.setUsername(AuthHelper.VALID_ACCOUNT, "takenusername"))
        .thenThrow(new UsernameNotAvailableException());

    when(changeNumberManager.changeNumber(any(), any(), any(), any())).thenAnswer((Answer<Account>) invocation -> {
      final Account account = invocation.getArgument(0, Account.class);
      final String number = invocation.getArgument(1, String.class);

      final UUID uuid = account.getUuid();
      final Set<Device> devices = account.getDevices();

      final Account updatedAccount = mock(Account.class);
      when(updatedAccount.getUuid()).thenReturn(uuid);
      when(updatedAccount.getNumber()).thenReturn(number);
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

      when(dynamicConfiguration.getCaptchaConfiguration()).thenReturn(signupCaptchaConfig);
    }
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(ABUSIVE_HOST))).thenReturn(Collections.singletonList(new AbusiveHostRule(ABUSIVE_HOST, true, Collections.emptyList())));
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(RESTRICTED_HOST))).thenReturn(Collections.singletonList(new AbusiveHostRule(RESTRICTED_HOST, false, Collections.singletonList("+123"))));
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(NICE_HOST))).thenReturn(Collections.emptyList());

    when(recaptchaClient.verify(eq(INVALID_CAPTCHA_TOKEN), anyString())).thenReturn(false);
    when(recaptchaClient.verify(eq(VALID_CAPTCHA_TOKEN), anyString())).thenReturn(true);

    doThrow(new RateLimitExceededException(Duration.ZERO)).when(pinLimiter).validate(eq(SENDER_OVER_PIN));

    doThrow(new RateLimitExceededException(Duration.ZERO)).when(autoBlockLimiter).validate(eq(RATE_LIMITED_PREFIX_HOST));
    doThrow(new RateLimitExceededException(Duration.ZERO)).when(autoBlockLimiter).validate(eq(RATE_LIMITED_IP_HOST));

    doThrow(new RateLimitExceededException(Duration.ZERO)).when(smsVoicePrefixLimiter).validate(SENDER_OVER_PREFIX.substring(0, 4+2));
    doThrow(new RateLimitExceededException(Duration.ZERO)).when(smsVoiceIpLimiter).validate(RATE_LIMITED_IP_HOST);
    doThrow(new RateLimitExceededException(Duration.ZERO)).when(smsVoiceIpLimiter).validate(RATE_LIMITED_HOST2);
  }

  @AfterEach
  void teardown() {
    reset(
        pendingAccountsManager,
        accountsManager,
        abusiveHostRules,
        rateLimiters,
        rateLimiter,
        pinLimiter,
        smsVoiceIpLimiter,
        smsVoicePrefixLimiter,
        autoBlockLimiter,
        usernameSetLimiter,
        smsSender,
        turnTokenGenerator,
        senderPinAccount,
        senderRegLockAccount,
        senderHasStorage,
        senderTransfer,
        recaptchaClient,
        gcmSender,
        apnSender,
        verifyExperimentEnrollmentManager,
        changeNumberManager);

    clearInvocations(AuthHelper.DISABLED_DEVICE);
  }

  @Test
  void testGetFcmPreauth() throws Exception {
    Response response = resources.getJerseyTest()
                                 .target("/v1/accounts/fcm/preauth/mytoken/+14152222222")
                                 .request()
                                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    ArgumentCaptor<GcmMessage> captor = ArgumentCaptor.forClass(GcmMessage.class);

    verify(gcmSender, times(1)).sendMessage(captor.capture());
    assertThat(captor.getValue().getGcmId()).isEqualTo("mytoken");
    assertThat(captor.getValue().getData().isPresent()).isTrue();
    assertThat(captor.getValue().getData().get().length()).isEqualTo(32);

    verifyNoMoreInteractions(apnSender);
  }

  @Test
  void testGetFcmPreauthIvoryCoast() throws Exception {
    Response response = resources.getJerseyTest()
            .target("/v1/accounts/fcm/preauth/mytoken/+2250707312345")
            .request()
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    ArgumentCaptor<GcmMessage> captor = ArgumentCaptor.forClass(GcmMessage.class);

    verify(gcmSender, times(1)).sendMessage(captor.capture());
    assertThat(captor.getValue().getGcmId()).isEqualTo("mytoken");
    assertThat(captor.getValue().getData().isPresent()).isTrue();
    assertThat(captor.getValue().getData().get().length()).isEqualTo(32);

    verifyNoMoreInteractions(apnSender);
  }

  @Test
  void testGetApnPreauth() throws Exception {
    Response response = resources.getJerseyTest()
                                 .target("/v1/accounts/apn/preauth/mytoken/+14152222222")
                                 .request()
                                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    ArgumentCaptor<ApnMessage> captor = ArgumentCaptor.forClass(ApnMessage.class);

    verify(apnSender, times(1)).sendMessage(captor.capture());
    assertThat(captor.getValue().getApnId()).isEqualTo("mytoken");
    assertThat(captor.getValue().getChallengeData().isPresent()).isTrue();
    assertThat(captor.getValue().getChallengeData().get().length()).isEqualTo(32);
    assertThat(captor.getValue().getMessage()).contains("\"challenge\" : \"" + captor.getValue().getChallengeData().get() + "\"");
    assertThat(captor.getValue().isVoip()).isTrue();

    verifyNoMoreInteractions(gcmSender);
  }

  @Test
  void testGetApnPreauthExplicitVoip() throws Exception {
    Response response = resources.getJerseyTest()
        .target("/v1/accounts/apn/preauth/mytoken/+14152222222")
        .queryParam("voip", "true")
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(200);

    ArgumentCaptor<ApnMessage> captor = ArgumentCaptor.forClass(ApnMessage.class);

    verify(apnSender, times(1)).sendMessage(captor.capture());
    assertThat(captor.getValue().getApnId()).isEqualTo("mytoken");
    assertThat(captor.getValue().getChallengeData().isPresent()).isTrue();
    assertThat(captor.getValue().getChallengeData().get().length()).isEqualTo(32);
    assertThat(captor.getValue().getMessage()).contains("\"challenge\" : \"" + captor.getValue().getChallengeData().get() + "\"");
    assertThat(captor.getValue().isVoip()).isTrue();

    verifyNoMoreInteractions(gcmSender);
  }

  @Test
  void testGetApnPreauthExplicitNoVoip() throws Exception {
    Response response = resources.getJerseyTest()
        .target("/v1/accounts/apn/preauth/mytoken/+14152222222")
        .queryParam("voip", "false")
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(200);

    ArgumentCaptor<ApnMessage> captor = ArgumentCaptor.forClass(ApnMessage.class);

    verify(apnSender, times(1)).sendMessage(captor.capture());
    assertThat(captor.getValue().getApnId()).isEqualTo("mytoken");
    assertThat(captor.getValue().getChallengeData().isPresent()).isTrue();
    assertThat(captor.getValue().getChallengeData().get().length()).isEqualTo(32);
    assertThat(captor.getValue().getMessage()).contains("\"challenge\" : \"" + captor.getValue().getChallengeData().get() + "\"");
    assertThat(captor.getValue().isVoip()).isFalse();

    verifyNoMoreInteractions(gcmSender);
  }

  @Test
  void testGetPreauthImpossibleNumber() {
    final Response response = resources.getJerseyTest()
        .target("/v1/accounts/fcm/preauth/mytoken/BogusNumber")
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isBlank();

    verifyNoMoreInteractions(gcmSender);
    verifyNoMoreInteractions(apnSender);
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

    verifyNoMoreInteractions(gcmSender);
    verifyNoMoreInteractions(apnSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendCode(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      ArgumentCaptor<StoredVerificationCode> storedVerificationCodeArgumentCaptor = ArgumentCaptor
          .forClass(StoredVerificationCode.class);

      verify(smsSender).deliverSmsVerificationWithTwilioVerify(eq(SENDER), eq(Optional.empty()), anyString(), eq(Collections.emptyList()));
      verify(pendingAccountsManager, times(2)).store(eq(SENDER), storedVerificationCodeArgumentCaptor.capture());

      assertThat(storedVerificationCodeArgumentCaptor.getValue().getTwilioVerificationSid())
          .isEqualTo(Optional.of("VerificationSid"));

    } else {
      verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.empty()), anyString());
    }
    verifyNoMoreInteractions(smsSender);
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }

  @Test
  void testSendCodeImpossibleNumber() {
    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", "Definitely not a real number"))
            .queryParam("challenge", "1234-push")
            .request()
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isBlank();

    verify(smsSender, never()).deliverSmsVerification(any(), any(), any());
  }

  @Test
  void testSendCodeNonNormalized() {
    final String number = "+4407700900111";

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", number))
            .queryParam("challenge", "1234-push")
            .request()
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(400);

    final NonNormalizedPhoneNumberResponse responseEntity = response.readEntity(NonNormalizedPhoneNumberResponse.class);
    assertThat(responseEntity.getOriginalNumber()).isEqualTo(number);
    assertThat(responseEntity.getNormalizedNumber()).isEqualTo("+447700900111");

    verify(smsSender, never()).deliverSmsVerification(any(), any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSendCodeVoiceNoLocale(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverVoxVerificationWithTwilioVerify(anyString(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/voice/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverVoxVerificationWithTwilioVerify(eq(SENDER), anyString(), eq(Collections.emptyList()));
    } else {
      verify(smsSender).deliverVoxVerification(eq(SENDER), anyString(), eq(Collections.emptyList()));
    }
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSendCodeVoiceSingleLocale(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverVoxVerificationWithTwilioVerify(anyString(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/voice/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header("Accept-Language", "pt-BR")
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender)
          .deliverVoxVerificationWithTwilioVerify(eq(SENDER), anyString(), eq(Locale.LanguageRange.parse("pt-BR")));
    } else {
      verify(smsSender).deliverVoxVerification(eq(SENDER), anyString(), eq(Locale.LanguageRange.parse("pt-BR")));
    }
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSendCodeVoiceMultipleLocales(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverVoxVerificationWithTwilioVerify(anyString(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/voice/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header("Accept-Language", "en-US;q=1, ar-US;q=0.9, fa-US;q=0.8, zh-Hans-US;q=0.7, ru-RU;q=0.6, zh-Hant-US;q=0.5")
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverVoxVerificationWithTwilioVerify(eq(SENDER), anyString(), eq(Locale.LanguageRange
          .parse("en-US;q=1, ar-US;q=0.9, fa-US;q=0.8, zh-Hans-US;q=0.7, ru-RU;q=0.6, zh-Hant-US;q=0.5")));
    } else {
      verify(smsSender).deliverVoxVerification(eq(SENDER), anyString(), eq(Locale.LanguageRange
          .parse("en-US;q=1, ar-US;q=0.9, fa-US;q=0.8, zh-Hans-US;q=0.7, ru-RU;q=0.6, zh-Hant-US;q=0.5")));
    }
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }

  @Test
  void testSendCodeVoiceInvalidLocale() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/voice/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header("Accept-Language", "This is not a reasonable Accept-Language value")
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(400);

    verify(smsSender, never()).deliverVoxVerification(eq(SENDER), anyString(), any());
    verify(smsSender, never()).deliverVoxVerificationWithTwilioVerify(eq(SENDER), anyString(), any());
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendCodeWithValidPreauth(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_PREAUTH))
                 .queryParam("challenge", "validchallenge")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverSmsVerificationWithTwilioVerify(eq(SENDER_PREAUTH), eq(Optional.empty()), anyString(),
          eq(Collections.emptyList()));
    } else {
      verify(smsSender).deliverSmsVerification(eq(SENDER_PREAUTH), eq(Optional.empty()), anyString());
    }
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }

  @Test
  void testSendCodeWithInvalidPreauth() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_PREAUTH))
                 .queryParam("challenge", "invalidchallenge")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verifyNoMoreInteractions(smsSender);
    verifyNoMoreInteractions(abusiveHostRules);
  }

  @Test
  void testSendCodeWithNoPreauth() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_PREAUTH))
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(smsSender, never()).deliverSmsVerification(eq(SENDER_PREAUTH), eq(Optional.empty()), anyString());
    verify(smsSender, never()).deliverSmsVerificationWithTwilioVerify(eq(SENDER_PREAUTH), eq(Optional.empty()), anyString(), anyList());
  }


  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendiOSCode(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("client", "ios")
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverSmsVerificationWithTwilioVerify(eq(SENDER), eq(Optional.of("ios")), anyString(),
          eq(Collections.emptyList()));
    } else {
      verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.of("ios")), anyString());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendAndroidNgCode(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("client", "android-ng")
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverSmsVerificationWithTwilioVerify(eq(SENDER), eq(Optional.of("android-ng")), anyString(),
          eq(Collections.emptyList()));
    } else {
      verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.of("android-ng")), anyString());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendAbusiveHost(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", ABUSIVE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(ABUSIVE_HOST));
    verifyNoMoreInteractions(smsSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendAbusiveHostWithValidCaptcha(final boolean enrolledInVerifyExperiment) throws IOException {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("captcha", VALID_CAPTCHA_TOKEN)
                 .request()
                 .header("X-Forwarded-For", ABUSIVE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verifyNoMoreInteractions(abusiveHostRules);
    verify(recaptchaClient).verify(eq(VALID_CAPTCHA_TOKEN), eq(ABUSIVE_HOST));
    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverSmsVerificationWithTwilioVerify(eq(SENDER), eq(Optional.empty()), anyString(),
          eq(Collections.emptyList()));
    } else {
      verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.empty()), anyString());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendAbusiveHostWithInvalidCaptcha(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("captcha", INVALID_CAPTCHA_TOKEN)
                 .request()
                 .header("X-Forwarded-For", ABUSIVE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verifyNoMoreInteractions(abusiveHostRules);
    verify(recaptchaClient).verify(eq(INVALID_CAPTCHA_TOKEN), eq(ABUSIVE_HOST));
    verifyNoMoreInteractions(smsSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendRateLimitedHostAutoBlock(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", RATE_LIMITED_IP_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(RATE_LIMITED_IP_HOST));
    verify(abusiveHostRules).setBlockedHost(eq(RATE_LIMITED_IP_HOST), eq("Auto-Block"));
    verifyNoMoreInteractions(abusiveHostRules);

    verifyNoMoreInteractions(recaptchaClient);
    verifyNoMoreInteractions(smsSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendRateLimitedPrefixAutoBlock(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER_OVER_PREFIX))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", RATE_LIMITED_PREFIX_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(RATE_LIMITED_PREFIX_HOST));
    verify(abusiveHostRules).setBlockedHost(eq(RATE_LIMITED_PREFIX_HOST), eq("Auto-Block"));
    verifyNoMoreInteractions(abusiveHostRules);

    verifyNoMoreInteractions(recaptchaClient);
    verifyNoMoreInteractions(smsSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendRateLimitedHostNoAutoBlock(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", RATE_LIMITED_HOST2)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(RATE_LIMITED_HOST2));
    verifyNoMoreInteractions(abusiveHostRules);

    verifyNoMoreInteractions(recaptchaClient);
    verifyNoMoreInteractions(smsSender);
  }


  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendMultipleHost(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST + ", " + ABUSIVE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(abusiveHostRules, times(1)).getAbusiveHostRulesFor(eq(ABUSIVE_HOST));

    verifyNoMoreInteractions(abusiveHostRules);
    verifyNoMoreInteractions(smsSender);
  }


  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendRestrictedHostOut(final boolean enrolledInVerifyExperiment) {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("challenge", "1234-push")
                 .request()
                 .header("X-Forwarded-For", RESTRICTED_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(402);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(RESTRICTED_HOST));
    verifyNoMoreInteractions(smsSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSendRestrictedIn(final boolean enrolledInVerifyExperiment) throws Exception {

    when(verifyExperimentEnrollmentManager.isEnrolled(any(), anyString(), anyList(), anyString()))
        .thenReturn(enrolledInVerifyExperiment);

    if (enrolledInVerifyExperiment) {
      when(smsSender.deliverSmsVerificationWithTwilioVerify(anyString(), any(), anyString(), anyList()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of("VerificationSid")));
    }

    final String number = "+12345678901";
    final String challenge = "challenge";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(new StoredVerificationCode("123456", System.currentTimeMillis(), challenge, null)));

    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", number))
                 .queryParam("challenge", challenge)
                 .request()
                 .header("X-Forwarded-For", RESTRICTED_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    if (enrolledInVerifyExperiment) {
      verify(smsSender).deliverSmsVerificationWithTwilioVerify(eq(number), eq(Optional.empty()), anyString(),
          eq(Collections.emptyList()));
    } else {
      verify(smsSender).deliverSmsVerification(eq(number), eq(Optional.empty()), anyString());
    }

    verifyNoMoreInteractions(smsSender);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testVerifyCode(final boolean enrolledInVerifyExperiment) throws Exception {
    if (enrolledInVerifyExperiment) {
      when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(
          Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis(), "1234-push", "VerificationSid")));;
    }

    resources.getJerseyTest()
             .target(String.format("/v1/accounts/code/%s", "1234"))
             .request()
             .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER, "bar"))
             .put(Entity.entity(new AccountAttributes(), MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(accountsManager).create(eq(SENDER), eq("bar"), any(), any(), anyList());

    if (enrolledInVerifyExperiment) {
      verify(smsSender).reportVerificationSucceeded("VerificationSid");
    }
  }

  @Test
  void testVerifyCodeBadCredentials() {
    final Response response = resources.getJerseyTest()
        .target(String.format("/v1/accounts/code/%s", "1234"))
        .request()
        .header("Authorization", "This is not a valid authorization header")
        .put(Entity.entity(new AccountAttributes(), MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testVerifyCodeOld() {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "1234"))
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_OLD, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  void testVerifyBadCode() {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "1111"))
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  void testVerifyRegistrationLock() throws Exception {
    AccountIdentityResponse result =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "666666"))
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, Hex.toStringCondensed(registration_lock_key), true, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    assertThat(result.getUuid()).isNotNull();

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));
  }

  @Test
  void testVerifyRegistrationLockSetsRegistrationLockOnNewAccount() throws Exception {
    AccountIdentityResponse result =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "666666"))
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, Hex.toStringCondensed(registration_lock_key), true, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    assertThat(result.getUuid()).isNotNull();

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

      AccountIdentityResponse result =
          resources.getJerseyTest()
              .target(String.format("/v1/accounts/code/%s", "666666"))
              .request()
              .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
              .put(Entity.entity(new AccountAttributes(false, 3333, null, null, true, null),
                  MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

      assertThat(result.getUuid()).isNotNull();

      verifyNoMoreInteractions(pinLimiter);
    } finally {
      when(senderRegLockAccount.getRegistrationLock()).thenReturn(lock);
    }
  }

  @Test
  void testVerifyWrongRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "666666"))
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null,
                    Hex.toStringCondensed(new byte[32]), true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));
  }

  @Test
  void testVerifyNoRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "666666"))
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_REG_LOCK, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 3333, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    RegistrationLockFailure failure = response.readEntity(RegistrationLockFailure.class);
    assertThat(failure.getBackupCredentials()).isNotNull();
    assertThat(failure.getBackupCredentials().getUsername()).isEqualTo(SENDER_REG_LOCK_UUID.toString());
    assertThat(failure.getBackupCredentials().getPassword()).isNotEmpty();
    assertThat(failure.getBackupCredentials().getPassword().startsWith(SENDER_REG_LOCK_UUID.toString())).isTrue();
    assertThat(failure.getTimeRemaining()).isGreaterThan(0);

    verifyNoMoreInteractions(pinLimiter);
  }

  @Test
  void testVerifyTransferSupported() {
    when(senderTransfer.isTransferSupported()).thenReturn(true);

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "1234"))
            .queryParam("transfer", true)
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_TRANSFER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testVerifyTransferNotSupported() {
    when(senderTransfer.isTransferSupported()).thenReturn(false);

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "1234"))
            .queryParam("transfer", true)
            .request()
            .header("Authorization", AuthHelper.getProvisioningAuthHeader(SENDER_TRANSFER, "bar"))
            .put(Entity.entity(new AccountAttributes(false, 2222, null, null, true, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testVerifyTransferSupportedNotRequested() {
    when(senderTransfer.isTransferSupported()).thenReturn(true);

    final Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/code/%s", "1234"))
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

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final AccountIdentityResponse accountIdentityResponse =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(number), any(), any());

    assertThat(accountIdentityResponse.getUuid()).isEqualTo(AuthHelper.VALID_UUID);
    assertThat(accountIdentityResponse.getNumber()).isEqualTo(number);
    assertThat(accountIdentityResponse.getPni()).isNotEqualTo(AuthHelper.VALID_PNI);
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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isBlank();
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);

    final NonNormalizedPhoneNumberResponse responseEntity = response.readEntity(NonNormalizedPhoneNumberResponse.class);
    assertThat(responseEntity.getOriginalNumber()).isEqualTo(number);
    assertThat(responseEntity.getNormalizedNumber()).isEqualTo("+447700900111");

    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberSameNumber() throws Exception {
    final AccountIdentityResponse accountIdentityResponse =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(AuthHelper.VALID_NUMBER, "567890", null, null, null),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any(), any(), any());
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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberIncorrectCode() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code + "-incorrect", null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockNotRequired() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockRequiredNotProvided() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final StoredRegistrationLock existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(true);

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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockRequiredIncorrect() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final String reglock = "setec-astronomy";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final StoredRegistrationLock existingRegistrationLock = mock(StoredRegistrationLock.class);
    when(existingRegistrationLock.requiresClientRegistrationLock()).thenReturn(true);
    when(existingRegistrationLock.verify(anyString())).thenReturn(false);

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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, reglock, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberExistingAccountReglockRequiredCorrect() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";
    final String reglock = "setec-astronomy";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

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
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, reglock, null, null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberDeviceMessagesWithoutPrekeys() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(number, code, null,
                    List.of(new IncomingMessage(1, null, 1, 1, "foo")), null),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberChangePrekeysDeviceMessagesMismatchDeviceIDs() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    Device device2 = mock(Device.class);
    when(device2.getId()).thenReturn(2L);
    when(device2.isEnabled()).thenReturn(true);
    Device device3 = mock(Device.class);
    when(device3.getId()).thenReturn(3L);
    when(device3.isEnabled()).thenReturn(true);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(Set.of(AuthHelper.VALID_DEVICE, device2, device3));
    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(
                    number, code, null,
                    List.of(
                        new IncomingMessage(1, null, 2, 1, "foo"),
                        new IncomingMessage(1, null, 4, 1, "foo")),
                    Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey(), 3L, new SignedPreKey())),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(409);
    verify(changeNumberManager, never()).changeNumber(any(), any(), any(), any());
  }

  @Test
  void testChangePhoneNumberChangePrekeys() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    Device device2 = mock(Device.class);
    when(device2.getId()).thenReturn(2L);
    when(device2.isEnabled()).thenReturn(true);
    when(device2.getRegistrationId()).thenReturn(2);
    Device device3 = mock(Device.class);
    when(device3.getId()).thenReturn(3L);
    when(device3.isEnabled()).thenReturn(true);
    when(device3.getRegistrationId()).thenReturn(3);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(Set.of(AuthHelper.VALID_DEVICE, device2, device3));
    when(AuthHelper.VALID_ACCOUNT.getDevice(2L)).thenReturn(Optional.of(device2));
    when(AuthHelper.VALID_ACCOUNT.getDevice(3L)).thenReturn(Optional.of(device3));
    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    var deviceMessages = List.of(
            new IncomingMessage(1, null, 2, 2, "content2"),
            new IncomingMessage(1, null, 3, 3, "content3"));
    var deviceKeys = Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey(), 3L, new SignedPreKey());

    final AccountIdentityResponse accountIdentityResponse =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(
                    number, code, null,
                    deviceMessages,
                    deviceKeys),
                MediaType.APPLICATION_JSON_TYPE), AccountIdentityResponse.class);

    verify(changeNumberManager).changeNumber(eq(AuthHelper.VALID_ACCOUNT), eq(number), any(), any());

    assertThat(accountIdentityResponse.getUuid()).isEqualTo(AuthHelper.VALID_UUID);
    assertThat(accountIdentityResponse.getNumber()).isEqualTo(number);
    assertThat(accountIdentityResponse.getPni()).isNotEqualTo(AuthHelper.VALID_PNI);
  }

  @Test
  void testChangePhoneNumberChangePrekeysDeviceMessagesMismatchRegistrationID() throws Exception {
    final String number = "+18005559876";
    final String code = "987654";

    Device device2 = mock(Device.class);
    when(device2.getId()).thenReturn(2L);
    when(device2.isEnabled()).thenReturn(true);
    when(device2.getRegistrationId()).thenReturn(2);
    Device device3 = mock(Device.class);
    when(device3.getId()).thenReturn(3L);
    when(device3.isEnabled()).thenReturn(true);
    when(device3.getRegistrationId()).thenReturn(3);
    when(AuthHelper.VALID_ACCOUNT.getDevices()).thenReturn(Set.of(AuthHelper.VALID_DEVICE, device2, device3));
    when(AuthHelper.VALID_ACCOUNT.getDevice(2L)).thenReturn(Optional.of(device2));
    when(AuthHelper.VALID_ACCOUNT.getDevice(3L)).thenReturn(Optional.of(device3));
    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(
        new StoredVerificationCode(code, System.currentTimeMillis(), "push", null)));

    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/number")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.entity(new ChangePhoneNumberRequest(
                    number, code, null,
                    List.of(
                        new IncomingMessage(1, null, 2, 1, "foo"),
                        new IncomingMessage(1, null, 3, 1, "foo")),
                    Map.of(1L, new SignedPreKey(), 2L, new SignedPreKey(), 3L, new SignedPreKey())),
                MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(410);
    verify(accountsManager, never()).changeNumber(eq(AuthHelper.VALID_ACCOUNT), any());
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

    assertThat(pinCapture.getValue().length()).isEqualTo(40);
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

    assertThat(response.getUuid()).isEqualTo(AuthHelper.VALID_UUID);
  }

  @Test
  void testSetUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/n00bkiller")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testSetTakenUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/takenusername")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testSetInvalidUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/pаypal")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void testSetInvalidPrefixUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/0n00bkiller")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void testSetUsernameBadAuth() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/n00bkiller")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
                 .put(Entity.text(""));

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

    Response response =
        resources.getJerseyTest()
            .target(String.format("/v1/accounts/sms/code/%s", SENDER))
            .queryParam("challenge", "1234-push")
            .request()
            .header("X-Forwarded-For", NICE_HOST)
            .get();

    assertThat(response.getStatus()).isEqualTo(expectedResponseStatusCode);

    verify(smsSender, 200 == expectedResponseStatusCode ? times(1) : never())
        .deliverSmsVerification(eq(SENDER), eq(Optional.empty()), anyString());
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
        .header("X-Forwarded-For", "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(200);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", phoneNumberIdentifier))
        .request()
        .header("X-Forwarded-For", "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(200);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .header("X-Forwarded-For", "127.0.0.1")
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
        .header("X-Forwarded-For", "127.0.0.1")
        .head();

    assertThat(response.getStatus()).isEqualTo(413);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(Duration.ofSeconds(13).toSeconds()));
  }

  @Test
  void testAccountExistsNoForwardedFor() throws RateLimitExceededException {
    final Response response = resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .header("X-Forwarded-For", "")
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
        .header("X-Forwarded-For", "127.0.0.1")
        .head()
        .getStatus()).isEqualTo(400);
  }
}
