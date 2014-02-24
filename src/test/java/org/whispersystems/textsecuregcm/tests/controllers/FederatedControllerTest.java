package org.whispersystems.textsecuregcm.tests.controllers;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.FederationController;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.entities.IncomingMessageList;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;

import static com.yammer.dropwizard.testing.JsonHelpers.jsonFixture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FederatedControllerTest extends ResourceTest {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final String MULTI_DEVICE_RECIPIENT  = "+14152222222";

  private PushSender             pushSender             = mock(PushSender.class            );
  private FederatedClientManager federatedClientManager = mock(FederatedClientManager.class);
  private AccountsManager        accountsManager        = mock(AccountsManager.class       );
  private RateLimiters           rateLimiters           = mock(RateLimiters.class          );
  private RateLimiter            rateLimiter            = mock(RateLimiter.class           );

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  protected void setUpResources() throws Exception {
    addProvider(AuthHelper.getAuthenticator());

    List<Device> singleDeviceList = new LinkedList<Device>() {{
      add(new Device(1, "foo", "bar", "baz", "isgcm", null, false, 111));
    }};

    List<Device> multiDeviceList = new LinkedList<Device>() {{
      add(new Device(1, "foo", "bar", "baz", "isgcm", null, false, 222));
      add(new Device(2, "foo", "bar", "baz", "isgcm", null, false, 333));
    }};

    Account singleDeviceAccount = new Account(SINGLE_DEVICE_RECIPIENT, false, singleDeviceList);
    Account multiDeviceAccount  = new Account(MULTI_DEVICE_RECIPIENT, false, multiDeviceList);

    when(accountsManager.get(eq(SINGLE_DEVICE_RECIPIENT))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(eq(MULTI_DEVICE_RECIPIENT))).thenReturn(Optional.of(multiDeviceAccount));

    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);

    MessageController messageController = new MessageController(rateLimiters, pushSender, accountsManager, federatedClientManager);
    addResource(new FederationController(accountsManager, null, null, messageController));
  }

  @Test
  public void testSingleDeviceCurrent() throws Exception {
    ClientResponse response =
        client().resource(String.format("/v1/federation/messages/+14152223333/1/%s", SINGLE_DEVICE_RECIPIENT))
            .header("Authorization", AuthHelper.getAuthHeader("cyanogen", "foofoo"))
            .entity(mapper.readValue(jsonFixture("fixtures/current_message_single_device.json"), IncomingMessageList.class))
            .type(MediaType.APPLICATION_JSON_TYPE)
            .put(ClientResponse.class);

    assertThat("Good Response", response.getStatus(), is(equalTo(204)));

    verify(pushSender).sendMessage(any(Account.class), any(Device.class), any(MessageProtos.OutgoingMessageSignal.class));
  }


}
