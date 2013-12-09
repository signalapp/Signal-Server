package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.sms.SenderFactory;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.core.MediaType;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AccountControllerTest extends ResourceTest {

  private static final String SENDER = "+14152222222";

  private PendingAccountsManager pendingAccountsManager = mock(PendingAccountsManager.class);
  private AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private RateLimiter            rateLimiter            = mock(RateLimiter.class           );
  private TwilioSmsSender        twilioSmsSender        = mock(TwilioSmsSender.class       );
  private SenderFactory          senderFactory          = mock(SenderFactory.class         );

  @Override
  protected void setUpResources() throws Exception {
    addProvider(AuthHelper.getAuthenticator());

    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of("1234"));
    when(senderFactory.getSmsSender(SENDER)).thenReturn(twilioSmsSender);

    addResource(new AccountController(pendingAccountsManager,
                                      accountsManager,
                                      rateLimiters,
                                      senderFactory));
  }

  @Test
  public void testSendCode() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/accounts/sms/code/%s", SENDER))
            .get(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(200);

    verify(twilioSmsSender).deliverSmsVerification(eq(SENDER), anyString());
  }

  @Test
  public void testVerifyCode() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/accounts/code/%s", "1234"))
            .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
            .entity(new AccountAttributes("keykeykeykey", false))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager).create(isA(Account.class));
  }

  @Test
  public void testVerifyBadCode() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/accounts/code/%s", "1111"))
            .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
            .entity(new AccountAttributes("keykeykeykey", false))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

}
