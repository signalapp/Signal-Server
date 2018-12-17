package org.whispersystems.textsecuregcm.tests.controllers;

import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.providers.TimeProvider;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AccountControllerTest {

  private static final String SENDER          = "+14152222222";
  private static final String SENDER_OLD      = "+14151111111";
  private static final String SENDER_PIN      = "+14153333333";
  private static final String SENDER_OVER_PIN = "+14154444444";

  private static final String ABUSIVE_HOST    = "192.168.1.1";
  private static final String RESTRICTED_HOST = "192.168.1.2";
  private static final String NICE_HOST       = "127.0.0.1";

  private        PendingAccountsManager pendingAccountsManager = mock(PendingAccountsManager.class);
  private        AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private        AbusiveHostRules       abusiveHostRules       = mock(AbusiveHostRules.class      );
  private        RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private        RateLimiter            rateLimiter            = mock(RateLimiter.class           );
  private        RateLimiter            pinLimiter             = mock(RateLimiter.class           );
  private        RateLimiter            smsVoiceIpLimiter      = mock(RateLimiter.class           );
  private        SmsSender              smsSender              = mock(SmsSender.class             );
  private        DirectoryQueue         directoryQueue         = mock(DirectoryQueue.class);
  private        MessagesManager        storedMessages         = mock(MessagesManager.class       );
  private        TimeProvider           timeProvider           = mock(TimeProvider.class          );
  private        TurnTokenGenerator     turnTokenGenerator     = mock(TurnTokenGenerator.class);
  private        Account                senderPinAccount       = mock(Account.class);

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new AuthValueFactoryProvider.Binder<>(Account.class))
                                                            .addProvider(new RateLimitExceededExceptionMapper())
                                                            .setMapper(SystemMapper.getMapper())
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new AccountController(pendingAccountsManager,
                                                                                               accountsManager,
                                                                                               abusiveHostRules,
                                                                                               rateLimiters,
                                                                                               smsSender,
                                                                                               directoryQueue,
                                                                                               storedMessages,
                                                                                               turnTokenGenerator,
                                                                                               new HashMap<>()))
                                                            .build();


  @Before
  public void setup() throws Exception {
    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getPinLimiter()).thenReturn(pinLimiter);
    when(rateLimiters.getSmsVoiceIpLimiter()).thenReturn(smsVoiceIpLimiter);

    when(timeProvider.getCurrentTimeMillis()).thenReturn(System.currentTimeMillis());

    when(senderPinAccount.getPin()).thenReturn(Optional.of("31337"));
    when(senderPinAccount.getLastSeen()).thenReturn(System.currentTimeMillis());

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis())));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OLD)).thenReturn(Optional.of(new StoredVerificationCode("1234", System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(31))));
    when(pendingAccountsManager.getCodeForNumber(SENDER_PIN)).thenReturn(Optional.of(new StoredVerificationCode("333333", System.currentTimeMillis())));
    when(pendingAccountsManager.getCodeForNumber(SENDER_OVER_PIN)).thenReturn(Optional.of(new StoredVerificationCode("444444", System.currentTimeMillis())));

    when(accountsManager.get(eq(SENDER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.get(eq(SENDER_OVER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.get(eq(SENDER))).thenReturn(Optional.empty());
    when(accountsManager.get(eq(SENDER_OLD))).thenReturn(Optional.empty());

    when(abusiveHostRules.getAbusiveHostRulesFor(eq(ABUSIVE_HOST))).thenReturn(Collections.singletonList(new AbusiveHostRule(ABUSIVE_HOST, true, Collections.emptyList())));
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(RESTRICTED_HOST))).thenReturn(Collections.singletonList(new AbusiveHostRule(RESTRICTED_HOST, false, Collections.singletonList("+123"))));
    when(abusiveHostRules.getAbusiveHostRulesFor(eq(NICE_HOST))).thenReturn(Collections.emptyList());

    doThrow(new RateLimitExceededException(SENDER_OVER_PIN)).when(pinLimiter).validate(eq(SENDER_OVER_PIN));
  }

  @Test
  public void testSendCode() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.empty()), anyString());
    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(NICE_HOST));
  }
  
  @Test
  public void testSendiOSCode() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("client", "ios")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.of("ios")), anyString());
  }

  @Test
  public void testSendAndroidNgCode() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .queryParam("client", "android-ng")
                 .request()
                 .header("X-Forwarded-For", NICE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(smsSender).deliverSmsVerification(eq(SENDER), eq(Optional.of("android-ng")), anyString());
  }

  @Test
  public void testSendAbusiveHost() {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .request()
                 .header("X-Forwarded-For", ABUSIVE_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(ABUSIVE_HOST));
    verifyNoMoreInteractions(smsSender);
  }

  @Test
  public void testSendRestrictedHostOut() {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", SENDER))
                 .request()
                 .header("X-Forwarded-For", RESTRICTED_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(abusiveHostRules).getAbusiveHostRulesFor(eq(RESTRICTED_HOST));
    verifyNoMoreInteractions(smsSender);
  }

  @Test
  public void testSendRestrictedIn() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/sms/code/%s", "+1234567890"))
                 .request()
                 .header("X-Forwarded-For", RESTRICTED_HOST)
                 .get();

    assertThat(response.getStatus()).isEqualTo(200);

    verify(smsSender).deliverSmsVerification(eq("+1234567890"), eq(Optional.empty()), anyString());
  }

  @Test
  public void testVerifyCode() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "1234"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 2222, null),
                               MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager, times(1)).create(isA(Account.class));
    verify(directoryQueue, times(1)).addRegisteredUser(eq(SENDER));
  }

  @Test
  public void testVerifyCodeOld() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "1234"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_OLD, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 2222, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  public void testVerifyBadCode() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "1111"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 3333, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  public void testVerifyPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "333333"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 3333, "31337"),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(pinLimiter).validate(eq(SENDER_PIN));
  }

  @Test
  public void testVerifyWrongPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "333333"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 3333, "31338"),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    verify(pinLimiter).validate(eq(SENDER_PIN));
  }

  @Test
  public void testVerifyNoPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "333333"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 3333, null),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(423);

    RegistrationLockFailure failure = response.readEntity(RegistrationLockFailure.class);

    verifyNoMoreInteractions(pinLimiter);
  }

  @Test
  public void testVerifyLimitPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/accounts/code/%s", "444444"))
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER_OVER_PIN, "bar"))
                 .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 3333, "31337"),
                                    MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(413);

    verify(rateLimiter).clear(eq(SENDER_OVER_PIN));
  }

  @Test
  public void testVerifyOldPin() throws Exception {
    try {
      when(senderPinAccount.getLastSeen()).thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7));

      Response response =
          resources.getJerseyTest()
                   .target(String.format("/v1/accounts/code/%s", "444444"))
                   .request()
                   .header("Authorization", AuthHelper.getAuthHeader(SENDER_OVER_PIN, "bar"))
                   .put(Entity.entity(new AccountAttributes("keykeykeykey", false, 3333, null),
                                      MediaType.APPLICATION_JSON_TYPE));

      assertThat(response.getStatus()).isEqualTo(204);

    } finally {
      when(senderPinAccount.getLastSeen()).thenReturn(System.currentTimeMillis());
    }
  }


  @Test
  public void testSetPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("31337")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_ACCOUNT).setPin(eq("31337"));
  }

  @Test
  public void testSetPinUnauthorized() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .put(Entity.json(new RegistrationLock("31337")));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  public void testSetShortPin() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/pin/")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("313")));

    assertThat(response.getStatus()).isEqualTo(422);

    verify(AuthHelper.VALID_ACCOUNT, never()).setPin(anyString());
  }



}
