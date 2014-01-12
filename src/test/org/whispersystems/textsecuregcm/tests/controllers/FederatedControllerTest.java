package org.whispersystems.textsecuregcm.tests.controllers;

import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.FederationController;
import org.whispersystems.textsecuregcm.controllers.KeysController;
import org.whispersystems.textsecuregcm.controllers.MissingDevicesException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
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
  private final String EXISTS_NUMBER_2   = "+14154444444";
  private final String NOT_EXISTS_NUMBER = "+14152222220";

  private final Keys       keys       = mock(Keys.class);
  private final PushSender pushSender = mock(PushSender.class);

  Device[] fakeDevice;
  Account existsAccount;

  @Override
  protected void setUpResources() throws MissingDevicesException {
    addProvider(AuthHelper.getAuthenticator());

    RateLimiters    rateLimiters = mock(RateLimiters.class);
    RateLimiter     rateLimiter  = mock(RateLimiter.class );
    AccountsManager accounts     = mock(AccountsManager.class);

    fakeDevice = new Device[2];
    fakeDevice[0] = new Device(42, EXISTS_NUMBER, 1, "", "", "", null, null, true, false);
    fakeDevice[1] = new Device(43, EXISTS_NUMBER, 2, "", "", "", null, null, false, true);
    existsAccount = new Account(EXISTS_NUMBER, true, Arrays.asList(fakeDevice[0], fakeDevice[1]));

    when(rateLimiters.getPreKeysLimiter()).thenReturn(rateLimiter);

    Map<String, Set<Long>> validOneElementSet = new HashMap<>();
    validOneElementSet.put(EXISTS_NUMBER_2, new HashSet<>(Arrays.asList((long) 1)));
    List<Account> validOneAccount = Arrays.asList(new Account(EXISTS_NUMBER_2, true,
                                                              Arrays.asList(new Device(44, EXISTS_NUMBER_2, 1, "", "", "", null, null, true, false))));

    Map<String, Set<Long>> validTwoElementsSet = new HashMap<>();
    validTwoElementsSet.put(EXISTS_NUMBER, new HashSet<>(Arrays.asList((long) 1, (long) 2)));
    List<Account> validTwoAccount = Arrays.asList(new Account(EXISTS_NUMBER, true, Arrays.asList(fakeDevice[0], fakeDevice[1])));

    Map<String, Set<Long>> invalidTwoElementsSet = new HashMap<>();
    invalidTwoElementsSet.put(EXISTS_NUMBER, new HashSet<>(Arrays.asList((long) 1)));

    when(accounts.getAccountsForDevices(eq(validOneElementSet))).thenReturn(validOneAccount);
    when(accounts.getAccountsForDevices(eq(validTwoElementsSet))).thenReturn(validTwoAccount);
    when(accounts.getAccountsForDevices(eq(invalidTwoElementsSet))).thenThrow(new MissingDevicesException(new HashSet<>(Arrays.asList(EXISTS_NUMBER))));

    addResource(new FederationController(keys, accounts, pushSender, mock(UrlSigner.class)));
  }

  @Test
  public void validRequestsTest() throws Exception {
     MessageResponse result = client().resource("/v1/federation/message")
        .entity(Arrays.asList(new RelayMessage(EXISTS_NUMBER_2, 1, MessageProtos.OutgoingMessageSignal.newBuilder().build().toByteArray())))
        .type(MediaType.APPLICATION_JSON)
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_FEDERATION_PEER, AuthHelper.FEDERATION_PEER_TOKEN))
        .put(MessageResponse.class);

    assertThat(result.getSuccess()).isEqualTo(Arrays.asList(EXISTS_NUMBER_2));
    assertThat(result.getFailure()).isEmpty();
    assertThat(result.getNumbersMissingDevices()).isEmpty();

    result = client().resource("/v1/federation/message")
        .entity(Arrays.asList(new RelayMessage(EXISTS_NUMBER, 1, MessageProtos.OutgoingMessageSignal.newBuilder().build().toByteArray()),
                              new RelayMessage(EXISTS_NUMBER, 2, MessageProtos.OutgoingMessageSignal.newBuilder().build().toByteArray())))
        .type(MediaType.APPLICATION_JSON)
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_FEDERATION_PEER, AuthHelper.FEDERATION_PEER_TOKEN))
        .put(MessageResponse.class);

    assertThat(result.getSuccess()).isEqualTo(Arrays.asList(EXISTS_NUMBER, EXISTS_NUMBER + "." + 2));
    assertThat(result.getFailure()).isEmpty();
    assertThat(result.getNumbersMissingDevices()).isEmpty();
  }

  @Test
  public void invalidRequestTest() throws Exception {
    MessageResponse     result = client().resource("/v1/federation/message")
        .entity(Arrays.asList(new RelayMessage(EXISTS_NUMBER, 1, MessageProtos.OutgoingMessageSignal.newBuilder().build().toByteArray())))
        .type(MediaType.APPLICATION_JSON)
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_FEDERATION_PEER, AuthHelper.FEDERATION_PEER_TOKEN))
        .put(MessageResponse.class);

    assertThat(result.getSuccess()).isEmpty();
    assertThat(result.getFailure()).isEqualTo(Arrays.asList(EXISTS_NUMBER));
    assertThat(result.getNumbersMissingDevices()).isEqualTo(new HashSet<>(Arrays.asList(EXISTS_NUMBER)));
  }
}