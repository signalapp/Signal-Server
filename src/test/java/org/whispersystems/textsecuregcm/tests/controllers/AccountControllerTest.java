package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.providers.TimeProvider;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
//import org.whispersystems.textsecuregcm.storage.StoredMessages;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.core.MediaType;

import java.util.HashMap;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AccountControllerTest {

  private static final String SENDER = "+14152222222";

  private        PendingAccountsManager pendingAccountsManager = mock(PendingAccountsManager.class);
  private        AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private        RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private        RateLimiter            rateLimiter            = mock(RateLimiter.class           );
  private        SmsSender              smsSender              = mock(SmsSender.class             );
  private        MessagesManager        storedMessages         = mock(MessagesManager.class       );
  private        TimeProvider           timeProvider           = mock(TimeProvider.class          );
  private static byte[]                 authorizationKey       = decodeHex("3a078586eea8971155f5c1ebd73c8c923cbec1c3ed22a54722e4e88321dc749f");

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthenticator())
                                                            .addResource(new AccountController(pendingAccountsManager,
                                                                                               accountsManager,
                                                                                               rateLimiters,
                                                                                               smsSender,
                                                                                               storedMessages,
                                                                                               timeProvider,
                                                                                               Optional.of(authorizationKey),
                                                                                               new HashMap<String, Integer>()))
                                                            .build();


  @Before
  public void setup() throws Exception {
    when(rateLimiters.getSmsDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVoiceDestinationLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getVerifyLimiter()).thenReturn(rateLimiter);

    when(timeProvider.getCurrentTimeMillis()).thenReturn(System.currentTimeMillis());

    when(pendingAccountsManager.getCodeForNumber(SENDER)).thenReturn(Optional.of("1234"));
  }

  @Test
  public void testSendCode() throws Exception {
    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/sms/code/%s", SENDER))
            .get(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(200);

    verify(smsSender).deliverSmsVerification(eq(SENDER), anyString());
  }

  @Test
  public void testVerifyCode() throws Exception {
    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/code/%s", "1234"))
            .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
            .entity(new AccountAttributes("keykeykeykey", false, 2222))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager, times(1)).create(isA(Account.class));
  }

  @Test
  public void testVerifyBadCode() throws Exception {
    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/code/%s", "1111"))
            .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
            .entity(new AccountAttributes("keykeykeykey", false, 3333))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  public void testVerifyToken() throws Exception {
    when(timeProvider.getCurrentTimeMillis()).thenReturn(1415917053106L);

    String token = SENDER + ":1415906573:af4f046107c21721224a";

    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/token/%s", token))
        .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
        .entity(new AccountAttributes("keykeykeykey", false, 4444))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(204);

    verify(accountsManager, times(1)).create(isA(Account.class));
  }

  @Test
  public void testVerifyBadToken() throws Exception {
    when(timeProvider.getCurrentTimeMillis()).thenReturn(1415917053106L);

    String token = SENDER + ":1415906574:af4f046107c21721224a";

    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/token/%s", token))
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                 .entity(new AccountAttributes("keykeykeykey", false, 4444))
                 .type(MediaType.APPLICATION_JSON_TYPE)
                 .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  public void testVerifyWrongToken() throws Exception {
    when(timeProvider.getCurrentTimeMillis()).thenReturn(1415917053106L);

    String token = SENDER + ":1415906573:af4f046107c21721224a";

    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/token/%s", token))
                 .header("Authorization", AuthHelper.getAuthHeader("+14151111111", "bar"))
                 .entity(new AccountAttributes("keykeykeykey", false, 4444))
                 .type(MediaType.APPLICATION_JSON_TYPE)
                 .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  public void testVerifyExpiredToken() throws Exception {
    when(timeProvider.getCurrentTimeMillis()).thenReturn(1416003757901L);

    String token = SENDER + ":1415906573:af4f046107c21721224a";

    ClientResponse response =
        resources.client().resource(String.format("/v1/accounts/token/%s", token))
                 .header("Authorization", AuthHelper.getAuthHeader(SENDER, "bar"))
                 .entity(new AccountAttributes("keykeykeykey", false, 4444))
                 .type(MediaType.APPLICATION_JSON_TYPE)
                 .put(ClientResponse.class);

    assertThat(response.getStatus()).isEqualTo(403);

    verifyNoMoreInteractions(accountsManager);
  }

  private static byte[] decodeHex(String hex) {
    try {
      return Hex.decodeHex(hex.toCharArray());
    } catch (DecoderException e) {
      throw new AssertionError(e);
    }
  }

}