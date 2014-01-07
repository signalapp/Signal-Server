package org.whispersystems.textsecuregcm.tests.controllers;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.UnstructuredPreKeyList;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.jws.WebResult;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class KeyControllerTest extends ResourceTest {

  private final String EXISTS_NUMBER     = "+14152222222";
  private final String NOT_EXISTS_NUMBER = "+14152222220";

  private final PreKey SAMPLE_KEY  = new PreKey(1, EXISTS_NUMBER, AuthHelper.DEFAULT_DEVICE_ID, 1234, "test1", "test2", false);
  private final PreKey SAMPLE_KEY2 = new PreKey(2, EXISTS_NUMBER, 2, 5667, "test3", "test4", false);
  private final Keys   keys        = mock(Keys.class);

  Account[] fakeAccount;

  @Override
  protected void setUpResources() {
    addProvider(AuthHelper.getAuthenticator());

    RateLimiters    rateLimiters = mock(RateLimiters.class);
    RateLimiter     rateLimiter  = mock(RateLimiter.class );
    AccountsManager accounts     = mock(AccountsManager.class);

    fakeAccount = new Account[2];
    fakeAccount[0] = mock(Account.class);
    fakeAccount[1] = mock(Account.class);

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(keys.get(eq(EXISTS_NUMBER), anyList())).thenReturn(new UnstructuredPreKeyList(Arrays.asList(SAMPLE_KEY, SAMPLE_KEY2)));
    when(keys.get(eq(NOT_EXISTS_NUMBER), anyList())).thenReturn(null);

    when(fakeAccount[0].getDeviceId()).thenReturn(AuthHelper.DEFAULT_DEVICE_ID);
    when(fakeAccount[1].getDeviceId()).thenReturn((long) 2);

    when(accounts.getAllByNumber(EXISTS_NUMBER)).thenReturn(Arrays.asList(fakeAccount[0], fakeAccount[1]));
    when(accounts.getAllByNumber(NOT_EXISTS_NUMBER)).thenReturn(new LinkedList<Account>());

    addResource(new KeysController.V1(rateLimiters, keys, accounts, null));
    addResource(new KeysController.V2(rateLimiters, keys, accounts, null));
  }

  @Test
  public void validRequestsTest() throws Exception {
    PreKey result = client().resource(String.format("/v1/keys/%s", EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(PreKey.class);

    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(SAMPLE_KEY.getIdentityKey());

    assertThat(result.getId() == 0);
    assertThat(result.getNumber() == null);

    verify(keys).get(eq(EXISTS_NUMBER), eq(Arrays.asList(fakeAccount)));
    verifyNoMoreInteractions(keys);

    List<PreKey> results = client().resource(String.format("/v2/keys/%s", EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(new GenericType<List<PreKey>>(){});

    assertThat(results.size()).isEqualTo(2);
    result = results.get(0);
    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(SAMPLE_KEY.getIdentityKey());

    assertThat(result.getId() == 0);
    assertThat(result.getNumber() == null);

    result = results.get(1);
    assertThat(result.getKeyId()).isEqualTo(SAMPLE_KEY2.getKeyId());
    assertThat(result.getPublicKey()).isEqualTo(SAMPLE_KEY2.getPublicKey());
    assertThat(result.getIdentityKey()).isEqualTo(SAMPLE_KEY2.getIdentityKey());

    assertThat(result.getId() == 1);
    assertThat(result.getNumber() == null);

    verify(keys, times(2)).get(eq(EXISTS_NUMBER), eq(Arrays.asList(fakeAccount[0], fakeAccount[1])));
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void invalidRequestTest() throws Exception {
    ClientResponse response = client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(404);

    verify(keys).get(NOT_EXISTS_NUMBER, new LinkedList<Account>());
  }

  @Test
  public void unauthorizedRequestTest() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
            .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(401);

    response =
        client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(401);
  }

}