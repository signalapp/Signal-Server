package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.FederationController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.entities.MessageResponse;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.entities.RelayMessage;
import org.whispersystems.textsecuregcm.entities.UnstructuredPreKeyList;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.UrlSigner;

import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class FederatedControllerTest extends ResourceTest {

  private final String EXISTS_NUMBER     = "+14152222222";
  private final String NOT_EXISTS_NUMBER = "+14152222220";

  private final PreKey SAMPLE_KEY  = new PreKey(1, EXISTS_NUMBER, AuthHelper.DEFAULT_DEVICE_ID, 1234, "test1", "test2", false);
  private final PreKey SAMPLE_KEY2 = new PreKey(2, EXISTS_NUMBER, 2, 5667, "test3", "test4", false);

  private final Keys       keys       = mock(Keys.class);
  private final PushSender pushSender = mock(PushSender.class);

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

    Account account = new Account(EXISTS_NUMBER, true, Arrays.asList(fakeDevice[0], fakeDevice[1]));

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    when(keys.get(eq(EXISTS_NUMBER), isA(Account.class))).thenReturn(new UnstructuredPreKeyList(Arrays.asList(SAMPLE_KEY, SAMPLE_KEY2)));
    when(keys.get(eq(NOT_EXISTS_NUMBER), isA(Account.class))).thenReturn(null);

    when(fakeDevice[0].getDeviceId()).thenReturn(AuthHelper.DEFAULT_DEVICE_ID);
    when(fakeDevice[1].getDeviceId()).thenReturn((long) 2);

    Map<String, Set<Long>> VALID_SET = new HashMap<>();
    VALID_SET.put(EXISTS_NUMBER, new HashSet<Long>(Arrays.asList((long)1)));
    Map<String, Account> VALID_ACCOUNTS = new HashMap<>();
    VALID_ACCOUNTS.put(EXISTS_NUMBER, account);

    Map<String, Set<Long>> INVALID_SET = new HashMap<>();
    INVALID_SET.put(NOT_EXISTS_NUMBER, new HashSet<Long>(Arrays.asList((long) 1)));

    when(accounts.getAccountsForDevices(eq(VALID_SET)))
        .thenReturn(new Pair<Map<String, Account>, List<String>>(VALID_ACCOUNTS, new LinkedList<String>()));
    when(accounts.getAccountsForDevices(eq(INVALID_SET)))
        .thenReturn(new Pair<Map<String, Account>, List<String>>(
            new HashMap<String, Account>(), Arrays.asList(NOT_EXISTS_NUMBER)));

    addResource(new FederationController(keys, accounts, pushSender, mock(UrlSigner.class)));
  }

  @Test
  public void validRequestsTest() throws Exception {
     MessageResponse result = client().resource("/v1/federation/message")
        .entity(new RelayMessage(EXISTS_NUMBER, 1, new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}))
        .type(MediaType.APPLICATION_JSON)
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_FEDERATION_PEER, AuthHelper.FEDERATION_PEER_TOKEN))
        .post(MessageResponse.class);

  }

  @Test
  public void invalidRequestTest() throws Exception {
    ClientResponse response = client().resource("/v1/federation/message")
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .post(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(404);
    verifyNoMoreInteractions(keys);
  }

  @Test
  public void unauthorizedRequestTest() throws Exception {
    ClientResponse response =
        client().resource("/v1/federation/message")
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.INVALID_PASSWORD))
            .post(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(401);

    response =
        client().resource(String.format("/v1/keys/%s", NOT_EXISTS_NUMBER))
            .post(ClientResponse.class);

    assertThat(response.getClientResponseStatus().getStatusCode()).isEqualTo(401);
  }

}