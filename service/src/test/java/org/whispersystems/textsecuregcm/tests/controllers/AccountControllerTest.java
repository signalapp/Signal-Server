/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicSignupCaptchaConfiguration;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountCreationResult;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeprecatedPin;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.GcmMessage;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioVerifyExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
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

  private static final String ABUSIVE_HOST             = "192.168.1.1";
  private static final String RESTRICTED_HOST          = "192.168.1.2";
  private static final String NICE_HOST                = "127.0.0.1";
  private static final String RATE_LIMITED_IP_HOST     = "10.0.0.1";
  private static final String RATE_LIMITED_PREFIX_HOST = "10.0.0.2";
  private static final String RATE_LIMITED_HOST2       = "10.0.0.3";

  private static final String VALID_CAPTCHA_TOKEN   = "valid_token";
  private static final String INVALID_CAPTCHA_TOKEN = "invalid_token";

  private static PendingAccountsManager pendingAccountsManager = mock(PendingAccountsManager.class);
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
  private static DirectoryQueue         directoryQueue         = mock(DirectoryQueue.class);
  private static MessagesManager        storedMessages         = mock(MessagesManager.class);
  private static TurnTokenGenerator     turnTokenGenerator     = mock(TurnTokenGenerator.class);
  private static Account                senderPinAccount       = mock(Account.class);
  private static Account                senderRegLockAccount   = mock(Account.class);
  private static Account                senderHasStorage       = mock(Account.class);
  private static Account                senderTransfer         = mock(Account.class);
  private static RecaptchaClient        recaptchaClient        = mock(RecaptchaClient.class);
  private static GCMSender              gcmSender              = mock(GCMSender.class);
  private static APNSender              apnSender              = mock(APNSender.class);
  private static UsernamesManager       usernamesManager       = mock(UsernamesManager.class);

  private static DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);

  private static TwilioVerifyExperimentEnrollmentManager verifyExperimentEnrollmentManager = mock(
      TwilioVerifyExperimentEnrollmentManager.class);

  private byte[] registration_lock_key = new byte[32];
  private static ExternalServiceCredentialGenerator storageCredentialGenerator = new ExternalServiceCredentialGenerator(new byte[32], new byte[32], false);

  private static final ResourceExtension resources = ResourceExtension.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
                                                                ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
                                                            .addProvider(new RateLimitExceededExceptionMapper())
                                                            .setMapper(SystemMapper.getMapper())
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new AccountController(pendingAccountsManager,
                                                                                               accountsManager,
                                                                                               usernamesManager,
                                                                                               abusiveHostRules,
                                                                                               rateLimiters,
                                                                                               smsSender,
                                                                                               directoryQueue,
                                                                                               storedMessages,
                                                                                               dynamicConfigurationManager,
                                                                                               turnTokenGenerator,
                                                                                               new HashMap<>(),
                                                                                               recaptchaClient,
                                                                                               gcmSender,
                                                                                               apnSender,
                                                                                               storageCredentialGenerator,
                                                                                               verifyExperimentEnrollmentManager))
                                                            .build();


  @BeforeEach
  void setup() throws Exception {
    clearInvocations(AuthHelper.VALID_ACCOUNT, AuthHelper.UNDISCOVERABLE_ACCOUNT);

    new SecureRandom().nextBytes(registration_lock_key);
    AuthenticationCredentials registrationLockCredentials = new AuthenticationCredentials(Hex.toStringCondensed(registration_lock_key));

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
    when(senderPinAccount.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), Optional.of("31337"), System.currentTimeMillis()));

    when(senderHasStorage.getUuid()).thenReturn(UUID.randomUUID());
    when(senderHasStorage.isStorageSupported()).thenReturn(true);
    when(senderHasStorage.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), Optional.empty(), System.currentTimeMillis()));

    when(senderRegLockAccount.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.of(registrationLockCredentials.getHashedAuthenticationToken()), Optional.of(registrationLockCredentials.getSalt()), Optional.empty(), System.currentTimeMillis()));
    when(senderRegLockAccount.getLastSeen()).thenReturn(System.currentTimeMillis());
    when(senderRegLockAccount.getUuid()).thenReturn(SENDER_REG_LOCK_UUID);

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis(), "1234-push")));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OLD)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(31), null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_PIN)).thenReturn(Optional.of(new StoredVerificationCode("333333", System.currentTimeMillis(), null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_REG_LOCK)).thenReturn(Optional.of(new StoredVerificationCode("666666", System.currentTimeMillis(), null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PIN)).thenReturn(Optional.of(new StoredVerificationCode("444444", System.currentTimeMillis(), null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PREFIX)).thenReturn(Optional.of(new StoredVerificationCode("777777", System.currentTimeMillis(), "1234-push")));
    when(pendingAccountsManager.getCodeForNumber(SENDER_PREAUTH)).thenReturn(Optional.of(new StoredVerificationCode("555555", System.currentTimeMillis(), "validchallenge")));
    when(pendingAccountsManager.getCodeForNumber(SENDER_HAS_STORAGE)).thenReturn(Optional.of(new StoredVerificationCode("666666", System.currentTimeMillis(), null)));
    when(pendingAccountsManager.getCodeForNumber(SENDER_TRANSFER)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis(), null)));

    when(accountsManager.get(eq(SENDER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.get(eq(SENDER_REG_LOCK))).thenReturn(Optional.of(senderRegLockAccount));
    when(accountsManager.get(eq(SENDER_OVER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.get(eq(SENDER))).thenReturn(Optional.empty());
    when(accountsManager.get(eq(SENDER_OLD))).thenReturn(Optional.empty());
    when(accountsManager.get(eq(SENDER_PREAUTH))).thenReturn(Optional.empty());
    when(accountsManager.get(eq(SENDER_HAS_STORAGE))).thenReturn(Optional.of(senderHasStorage));
    when(accountsManager.get(eq(SENDER_TRANSFER))).thenReturn(Optional.of(senderTransfer));

    when(usernamesManager.put(eq(AuthHelper.VALID_UUID), eq("n00bkiller"))).thenReturn(true);
    when(usernamesManager.put(eq(AuthHelper.VALID_UUID), eq("takenusername"))).thenReturn(false);

    {
      DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
      when(dynamicConfigurationManager.getConfiguration())
          .thenReturn(dynamicConfiguration);

      DynamicSignupCaptchaConfiguration signupCaptchaConfig = new DynamicSignupCaptchaConfiguration();

      when(dynamicConfiguration.getSignupCaptchaConfiguration()).thenReturn(signupCaptchaConfig);
    }
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(ABUSIVE_HOST))).thenReturn(Collections.singletonList(new AbusiveHostRule(ABUSIVE_HOST, true, Collections.emptyList())));
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(RESTRICTED_HOST))).thenReturn(Collections.singletonList(new AbusiveHostRule(RESTRICTED_HOST, false, Collections.singletonList("+123"))));
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(NICE_HOST))).thenReturn(Collections.emptyList());

    when(recaptchaClient.verify(eq(INVALID_CAPTCHA_TOKEN), anyString())).thenReturn(false);
    when(recaptchaClient.verify(eq(VALID_CAPTCHA_TOKEN), anyString())).thenReturn(true);

    doThrow(new RateLimitExceededException(SENDER_OVER_PIN, Duration.ZERO)).when(pinLimiter).validate(eq(SENDER_OVER_PIN));

    doThrow(new RateLimitExceededException(RATE_LIMITED_PREFIX_HOST, Duration.ZERO)).when(autoBlockLimiter).validate(eq(RATE_LIMITED_PREFIX_HOST));
    doThrow(new RateLimitExceededException(RATE_LIMITED_IP_HOST, Duration.ZERO)).when(autoBlockLimiter).validate(eq(RATE_LIMITED_IP_HOST));

    doThrow(new RateLimitExceededException(SENDER_OVER_PREFIX, Duration.ZERO)).when(smsVoicePrefixLimiter).validate(SENDER_OVER_PREFIX.substring(0, 4+2));
    doThrow(new RateLimitExceededException(RATE_LIMITED_IP_HOST, Duration.ZERO)).when(smsVoiceIpLimiter).validate(RATE_LIMITED_IP_HOST);
    doThrow(new RateLimitExceededException(RATE_LIMITED_HOST2, Duration.ZERO)).when(smsVoiceIpLimiter).validate(RATE_LIMITED_HOST2);
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
        directoryQueue,
        storedMessages,
        turnTokenGenerator,
        senderPinAccount,
        senderRegLockAccount,
        senderHasStorage,
        senderTransfer,
        recaptchaClient,
        gcmSender,
        apnSender,
        usernamesManager,
        verifyExperimentEnrollmentManager);
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

    verifyNoMoreInteractions(gcmSender);
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

    when(pendingAccountsManager.getCodeForNumber(number)).thenReturn(Optional.of(new StoredVerificationCode("123456", System.currentTimeMillis(), challenge)));

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

    AccountCreationResult result =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "1234"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, true, null),
                               MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

    assertThat(result.getUuid()).isNotNull();
    assertThat(result.isStorageCapable()).isFalse();

    final ArgumentCaptor<Account> accountArgumentCaptor = ArgumentCaptor.forClass(Account.class);

    verify(accountsManager, times(1)).create(accountArgumentCaptor.capture());
    verify(directoryQueue, times(1)).refreshRegisteredUser(argThat(account -> SENDER.equals(account.getNumber())));

    assertThat(accountArgumentCaptor.getValue().isDiscoverableByPhoneNumber()).isTrue();

    if (enrolledInVerifyExperiment) {
      verify(smsSender).reportVerificationSucceeded("VerificationSid");
    }
  }

  @Test
  void testVerifyCodeUndiscoverable() throws Exception {
    AccountCreationResult result =
            resources.getJerseyTest()
                    .target(String.format("/v1/accounts/code/%s", "1234"))
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                    .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, false, null),
                            MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

    assertThat(result.getUuid()).isNotNull();
    assertThat(result.isStorageCapable()).isFalse();

    final ArgumentCaptor<Account> accountArgumentCaptor = ArgumentCaptor.forClass(Account.class);

    verify(accountsManager, times(1)).create(accountArgumentCaptor.capture());
    verify(directoryQueue, times(1)).refreshRegisteredUser(argThat(account -> SENDER.equals(account.getNumber())));

    assertThat(accountArgumentCaptor.getValue().isDiscoverableByPhoneNumber()).isFalse();
  }

  @Test
  void testVerifySupportsStorage() throws Exception {
    AccountCreationResult result =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "666666"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_HAS_STORAGE, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

    assertThat(result.getUuid()).isNotNull();
    assertThat(result.isStorageCapable()).isTrue();

    verify(accountsManager, times(1)).create(isA(Account.class));
    verify(directoryQueue, times(1)).refreshRegisteredUser(argThat(account -> SENDER_HAS_STORAGE.equals(account.getNumber())));
  }

  @Test
  void testVerifyCodeOld() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "1234"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_OLD, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  void testVerifyBadCode() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "1111"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, null, null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  void testVerifyPin() throws Exception {
    AccountCreationResult result =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "333333"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, "31337", null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

    assertThat(result.getUuid()).isNotNull();

    verify(pinLimiter).validate(eq(SENDER_PIN));
  }

  @Test
  void testVerifyRegistrationLock() throws Exception {
    AccountCreationResult result =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "666666"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_REG_LOCK, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, null, Hex.toStringCondensed(registration_lock_key), true, null),
                                    MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

    assertThat(result.getUuid()).isNotNull();

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));
  }

  @Test
  void testVerifyRegistrationLockSetsRegistrationLockOnNewAccount() throws Exception {

    AccountCreationResult result =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "666666"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_REG_LOCK, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, null, Hex.toStringCondensed(registration_lock_key), true, null),
                                    MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

    assertThat(result.getUuid()).isNotNull();

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));

    verify(accountsManager).create(argThat(new ArgumentMatcher<>() {
      @Override
      public boolean matches(final Account account) {
        final StoredRegistrationLock regLock = account.getRegistrationLock();
        return regLock.requiresClientRegistrationLock() && regLock.verify(Hex.toStringCondensed(registration_lock_key), null);
      }

      @Override
      public String toString() {
        return "Account that has registration lock set";
      }
    }));

  }

  @Test
  void testVerifyRegistrationLockOld() throws Exception {
    StoredRegistrationLock lock = senderRegLockAccount.getRegistrationLock();

    try {
      when(senderRegLockAccount.getRegistrationLock()).thenReturn(lock.forTime(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7)));

      AccountCreationResult result =
          resources.getJerseyTest()
                   .target(String.format("/v1/accounts/code/%s", "666666"))
                   .request()
                   .header("Authorization", AuthHelper.getAuthHeader(SENDER_REG_LOCK, "bar"))
                   .put(Entity.entity(new AccountAttributes(false, 3333, null, null, null, true, null),
                                      MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

      assertThat(result.getUuid()).isNotNull();

      verifyNoMoreInteractions(pinLimiter);
    } finally {
      when(senderRegLockAccount.getRegistrationLock()).thenReturn(lock);
    }
  }

  @Test
  void testVerifyWrongPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "333333"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, "31338", null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    verify(pinLimiter).validate(eq(SENDER_PIN));
  }

  @Test
  void testVerifyWrongRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "666666"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_REG_LOCK, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null,
                         Hex.toStringCondensed(new byte[32]), null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    verify(pinLimiter).validate(eq(SENDER_REG_LOCK));
  }

  @Test
  void testVerifyNoPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "333333"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, null, null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    RegistrationLockFailure failure = response.readEntity(RegistrationLockFailure.class);
    assertThat(failure.getBackupCredentials()).isNull();

    verifyNoMoreInteractions(pinLimiter);
  }

  @Test
  void testVerifyNoRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "666666"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_REG_LOCK, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, null, null, true, null),
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
  void testVerifyLimitPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "444444"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_OVER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes(false, 3333, null, "31337", null, true, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(413);

    verify(rateLimiter).clear(eq(SENDER_OVER_PIN));
  }

  @Test
  void testVerifyOldPin() throws Exception {
    try {
      when(senderPinAccount.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), Optional.of("31337"), System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7)));

      AccountCreationResult result =
          resources.getJerseyTest()
                   .target(String.format("/v1/accounts/code/%s", "444444"))
                   .request()
                   .header("Authorization", AuthHelper.getAuthHeader(SENDER_OVER_PIN, "bar"))
                   .put(Entity.entity(new AccountAttributes(false, 3333, null, null, null, true, null),
                                      MediaType.APPLICATION_JSON_TYPE), AccountCreationResult.class);

      assertThat(result.getUuid()).isNotNull();

    } finally {
      when(senderPinAccount.getRegistrationLock()).thenReturn(new StoredRegistrationLock(Optional.empty(), Optional.empty(), Optional.of("31337"), System.currentTimeMillis()));
    }
  }

  @Test
  void testVerifyTransferSupported() {
    when(senderTransfer.isTransferSupported()).thenReturn(true);

    final Response response =
            resources.getJerseyTest()
                    .target(String.format("/v1/accounts/code/%s", "1234"))
                    .queryParam("transfer", true)
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(SENDER_TRANSFER, "bar"))
                    .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, true, null),
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
                    .header("Authorization", AuthHelper.getAuthHeader(SENDER_TRANSFER, "bar"))
                    .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, true, null),
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
                    .header("Authorization", AuthHelper.getAuthHeader(SENDER_TRANSFER, "bar"))
                    .put(Entity.entity(new AccountAttributes(false, 2222, null, null, null, true, null),
                            MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testSetPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new DeprecatedPin("31337")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_ACCOUNT).setPin(eq("31337"));
    verify(AuthHelper.VALID_ACCOUNT).setRegistrationLock(eq(null), eq(null));
  }

  @Test
  void testSetRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID.toString(), AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("1234567890123456789012345678901234567890123456789012345678901234")));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<String> pinCapture     = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> pinSaltCapture = ArgumentCaptor.forClass(String.class);

    verify(AuthHelper.VALID_ACCOUNT, times(1)).setPin(eq(null));
    verify(AuthHelper.VALID_ACCOUNT, times(1)).setRegistrationLock(pinCapture.capture(), pinSaltCapture.capture());

    assertThat(pinCapture.getValue()).isNotEmpty();
    assertThat(pinSaltCapture.getValue()).isNotEmpty();

    assertThat(pinCapture.getValue().length()).isEqualTo(40);
  }

  @Test
  void testSetPinUnauthorized() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .put(Entity.json(new DeprecatedPin("31337")));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetShortPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new DeprecatedPin("313")));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testSetShortRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("313")));

    assertThat(response.getStatus()).isEqualTo(422);
  }


  @Test
  void testSetPinDisabled() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new DeprecatedPin("31337")));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetRegistrationLockDisabled() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new RegistrationLock("1234567890123456789012345678901234567890123456789012345678901234")));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetGcmId() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/gcm/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new GcmRegistrationId("c00lz0rz")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setGcmId(eq("c00lz0rz"));
    verify(accountsManager, times(1)).update(eq(AuthHelper.DISABLED_ACCOUNT));
    verify(directoryQueue, never()).refreshRegisteredUser(any(Account.class));
  }

  @Test
  void testSetGcmIdByUuid() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/gcm/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID.toString(), AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new GcmRegistrationId("z000")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setGcmId(eq("z000"));
    verify(accountsManager, times(1)).update(eq(AuthHelper.DISABLED_ACCOUNT));
    verify(directoryQueue, never()).refreshRegisteredUser(any(Account.class));
  }

  @Test
  void testSetApnId() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/apn/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_NUMBER, AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new ApnRegistrationId("first", "second")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setApnId(eq("first"));
    verify(AuthHelper.DISABLED_DEVICE, times(1)).setVoipApnId(eq("second"));
    verify(accountsManager, times(1)).update(eq(AuthHelper.DISABLED_ACCOUNT));
    verify(directoryQueue, never()).refreshRegisteredUser(any(Account.class));
  }

  @Test
  void testSetApnIdByUuid() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/apn/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.DISABLED_UUID.toString(), AuthHelper.DISABLED_PASSWORD))
                 .put(Entity.json(new ApnRegistrationId("third", "fourth")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.DISABLED_DEVICE, times(1)).setApnId(eq("third"));
    verify(AuthHelper.DISABLED_DEVICE, times(1)).setVoipApnId(eq("fourth"));
    verify(accountsManager, times(1)).update(eq(AuthHelper.DISABLED_ACCOUNT));
    verify(directoryQueue, never()).refreshRegisteredUser(any(Account.class));
  }

  @ParameterizedTest
  @CsvSource("/v1/accounts/whoami/, /v1/accounts/me/")
  public void testWhoAmI(final String path) {
    AccountCreationResult response =
        resources.getJerseyTest()
                 .target(path)
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .get(AccountCreationResult.class);

    assertThat(response.getUuid()).isEqualTo(AuthHelper.VALID_UUID);
  }

  @Test
  void testSetUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/n00bkiller")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void testSetTakenUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/takenusername")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(409);
  }

  @Test
  void testSetInvalidUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/pаypal")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void testSetInvalidPrefixUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/0n00bkiller")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  void testSetUsernameBadAuth() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/n00bkiller")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
                 .put(Entity.text(""));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testDeleteUsername() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .delete();

    assertThat(response.getStatus()).isEqualTo(204);
    verify(usernamesManager, times(1)).delete(eq(AuthHelper.VALID_UUID));
  }

  @Test
  void testDeleteUsernameBadAuth() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
                 .delete();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetAccountAttributesNoDiscoverabilityChange() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, null, null, null, true, null)));

    assertThat(response.getStatus()).isEqualTo(204);
    verify(directoryQueue, never()).refreshRegisteredUser(any());
  }

  @Test
  void testSetAccountAttributesEnableDiscovery() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.UNDISCOVERABLE_NUMBER, AuthHelper.UNDISCOVERABLE_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, null, null, null, true, null)));

    assertThat(response.getStatus()).isEqualTo(204);
    verify(directoryQueue, times(1)).refreshRegisteredUser(AuthHelper.UNDISCOVERABLE_ACCOUNT);
  }

  @Test
  void testSetAccountAttributesDisableDiscovery() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, null, null, null, false, null)));

    assertThat(response.getStatus()).isEqualTo(204);
    verify(directoryQueue, times(1)).refreshRegisteredUser(AuthHelper.VALID_ACCOUNT);
  }

  @Test
  void testDeleteAccount() {
    Response response =
            resources.getJerseyTest()
                     .target("/v1/accounts/me")
                     .request()
                     .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                     .delete();

    assertThat(response.getStatus()).isEqualTo(204);
    verify(accountsManager).delete(AuthHelper.VALID_ACCOUNT, AccountsManager.DeletionReason.USER_REQUEST);
  }

  @ParameterizedTest
  @MethodSource
  void testSignupCaptcha(final String message, final boolean enforced, final Set<String> countryCodes, final int expectedResponseStatusCode) {
    DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    when(dynamicConfigurationManager.getConfiguration())
        .thenReturn(dynamicConfiguration);

    DynamicSignupCaptchaConfiguration signupCaptchaConfig = new DynamicSignupCaptchaConfiguration();
    signupCaptchaConfig.setCountryCodes(countryCodes);
    when(dynamicConfiguration.getSignupCaptchaConfiguration())
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
}
