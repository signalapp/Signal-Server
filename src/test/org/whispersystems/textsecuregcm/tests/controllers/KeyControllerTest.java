package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
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
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

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

  Device[] fakeDevice;
  Account existsAccount;

  @Override
  protected void setUpResources() {
    addProvider(AuthHelper.getAuthenticator());

    RateLimiters    rateLimiters = mock(RateLimiters.class);
    RateLimiter     rateLimiter  = mock(RateLimiter.class );
    AccountsManager accounts     = mock(AccountsManager.class);

    fakeDevice = new Device[2];
    fakeDevice[0] = mock(Device.class);
    fakeDevice[1] = mock(Device.class);
    existsAccount = new Account(EXISTS_NUMBER, true, Arrays.asList(fakeDevice[0], fakeDevice[1]));

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(keys.get(eq(EXISTS_NUMBER), isA(Account.class))).thenReturn(new UnstructuredPreKeyList(Arrays.asList(SAMPLE_KEY, SAMPLE_KEY2)));
    when(keys.get(eq(NOT_EXISTS_NUMBER), isA(Account.class))).thenReturn(null);

    when(fakeDevice[0].getDeviceId()).thenReturn(AuthHelper.DEFAULT_DEVICE_ID);
    when(fakeDevice[1].getDeviceId()).thenReturn((long) 2);

    when(accounts.getAccount(EXISTS_NUMBER)).thenReturn(Optional.of(existsAccount));
    when(accounts.getAccount(NOT_EXISTS_NUMBER)).thenReturn(Optional.<Account>absent());

    addResource(new KeysController(rateLimiters, keys, accounts, null));
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

    verify(keys).get(eq(EXISTS_NUMBER), eq(existsAccount));
    verifyNoMoreInteractions(keys);

    List<PreKey> results = client().resource(String.format("/v1/keys/%s?multikeys", EXISTS_NUMBER))
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

    verify(keys, times(2)).get(eq(EXISTS_NUMBER), eq(existsAccount));
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void invalidRequestTest() throws Exception {
    ClientResponse response = client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .get(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(404);
    verifyNoMoreInteractions(keys);
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