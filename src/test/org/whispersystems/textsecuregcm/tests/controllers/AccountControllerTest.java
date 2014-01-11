package org.whispersystems.textsecuregcm.tests.controllers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.DeviceController;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.VerificationCode;

import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AccountControllerTest extends ResourceTest {
  /** The AccountAttributes used in protocol v1 (no fetchesMessages) */
  static class V1AccountAttributes {
    @JsonProperty
    @NotEmpty
    private String signalingKey;

    @JsonProperty
    private boolean supportsSms;

    public V1AccountAttributes(String signalingKey, boolean supportsSms) {
      this.signalingKey = signalingKey;
      this.supportsSms  = supportsSms;
    }
  }

  @Path("/v1/accounts")
  static class DumbVerificationAccountController extends AccountController {
    public DumbVerificationAccountController(PendingAccountsManager pendingAccounts, AccountsManager accounts, RateLimiters rateLimiters, SmsSender smsSenderFactory) {
      super(pendingAccounts, accounts, rateLimiters, smsSenderFactory);
    }

    @Override
    protected VerificationCode generateVerificationCode() {
      return new VerificationCode(5678901);
    }
  }

  private static final String SENDER = "+14152222222";

  private PendingAccountsManager pendingAccountsManager = mock(PendingAccountsManager.class);
  private AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private RateLimiter            rateLimiter            = mock(RateLimiter.class           );
  private SmsSender              smsSender              = mock(SmsSender.class             );

  @Override
  protected void setUpResources() throws Exception {
    addProvider(AuthHelper.getAuthenticator());

    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of("1234"));

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        ((Account)invocation.getArguments()[0]).setDeviceId(2);
        return null;
      }
    }).when(accountsManager).createAccountOnExistingNumber(any(Account.class));

    addResource(new DumbVerificationAccountController(pendingAccountsManager, accountsManager, rateLimiters, smsSender));
  }

  @Test
  public void testSendCode() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/accounts/sms/code/%s", SENDER))
            .get(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(200);

    verify(smsSender).deliverSmsVerification(eq(SENDER), anyString());
  }

  @Test
  public void testVerifyCode() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/accounts/code/%s", "1234"))
            .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
            .entity(new V1AccountAttributes("keykeykeykey", false))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager).createResetNumber(isA(Account.class));

    ArgumentCaptor<String> number = ArgumentCaptor.forClass(String.class);
    verify(pendingAccountsManager).remove(number.capture());
    assertThat(number.getValue()).isEqualTo(SENDER);
  }

  @Test
  public void testVerifyBadCode() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/accounts/code/%s", "1111"))
            .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
            .entity(new V1AccountAttributes("keykeykeykey", false))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

}
