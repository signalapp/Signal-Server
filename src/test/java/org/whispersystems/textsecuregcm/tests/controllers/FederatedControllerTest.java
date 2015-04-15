package org.whispersystems.textsecuregcm.tests.controllers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.FederationControllerV1;
import org.whispersystems.textsecuregcm.controllers.FederationControllerV2;
import org.whispersystems.textsecuregcm.controllers.KeysControllerV2;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItemV2;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV2;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.core.MediaType;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

public class FederatedControllerTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final String MULTI_DEVICE_RECIPIENT  = "+14152222222";

  private PushSender             pushSender             = mock(PushSender.class            );
  private ReceiptSender          receiptSender          = mock(ReceiptSender.class);
  private FederatedClientManager federatedClientManager = mock(FederatedClientManager.class);
  private AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private MessagesManager        messagesManager        = mock(MessagesManager.class);
  private RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private RateLimiter            rateLimiter            = mock(RateLimiter.class           );

  private final SignedPreKey signedPreKey = new SignedPreKey(3333, "foo", "baar");
  private final PreKeyResponseV2 preKeyResponseV2 = new PreKeyResponseV2("foo", new LinkedList<PreKeyResponseItemV2>());

  private final ObjectMapper mapper = new ObjectMapper();

  private final MessageController messageController = new MessageController(rateLimiters, pushSender, receiptSender, accountsManager, messagesManager, federatedClientManager);
  private final KeysControllerV2  keysControllerV2  = mock(KeysControllerV2.class);

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthenticator())
                                                            .addResource(new FederationControllerV1(accountsManager, null, messageController, null))
                                                            .addResource(new FederationControllerV2(accountsManager, null, messageController, keysControllerV2))
                                                            .build();



  @Before
  public void setup() throws Exception {
    Set<Device> singleDeviceList = new HashSet<Device>() {{
      add(new Device(1, "foo", "bar", "baz", "isgcm", null, null, false, 111, null, System.currentTimeMillis()));
    }};

    Set<Device> multiDeviceList = new HashSet<Device>() {{
      add(new Device(1, "foo", "bar", "baz", "isgcm", null, null, false, 222, null, System.currentTimeMillis()));
      add(new Device(2, "foo", "bar", "baz", "isgcm", null, null, false, 333, null, System.currentTimeMillis()));
    }};

    Account singleDeviceAccount = new Account(SINGLE_DEVICE_RECIPIENT, false, singleDeviceList);
    Account multiDeviceAccount  = new Account(MULTI_DEVICE_RECIPIENT, false, multiDeviceList);

    when(accountsManager.get(eq(SINGLE_DEVICE_RECIPIENT))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(eq(MULTI_DEVICE_RECIPIENT))).thenReturn(Optional.of(multiDeviceAccount));

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);

    when(keysControllerV2.getSignedKey(any(Account.class))).thenReturn(Optional.of(signedPreKey));
    when(keysControllerV2.getDeviceKeys(any(Account.class), anyString(), anyString(), any(Optional.class)))
        .thenReturn(Optional.of(preKeyResponseV2));
  }

  @Test
  public void testSingleDeviceCurrent() throws Exception {
    ClientResponse response =
        resources.client().resource(String.format("/v1/federation/messages/+14152223333/1/%s", SINGLE_DEVICE_RECIPIENT))
            .header("Authorization", AuthHelper.getAuthHeader("cyanogen", "foofoo"))
            .entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat("Good Response", response.getStatus(), is(equalTo(204)));

    verify(pushSender).sendMessage(any(Account.class), any(Device.class), any(MessageProtos.OutgoingMessageSignal.class));
  }

  @Test
  public void testSignedPreKeyV2() throws Exception {
    PreKeyResponseV2 response =
        resources.client().resource("/v2/federation/key/+14152223333/1")
                 .header("Authorization", AuthHelper.getAuthHeader("cyanogen", "foofoo"))
                 .get(PreKeyResponseV2.class);

    assertThat("good response", response.getIdentityKey().equals(preKeyResponseV2.getIdentityKey()));
  }

}
