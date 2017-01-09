package org.whispersystems.textsecuregcm.tests.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.dropwizard.simpleauth.AuthValueFactoryProvider;
import org.whispersystems.textsecuregcm.controllers.ReceiptController;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;

import io.dropwizard.testing.junit.ResourceTestRule;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ReceiptControllerTest  {

  private static final String SINGLE_DEVICE_RECIPIENT = "+14151111111";
  private static final String MULTI_DEVICE_RECIPIENT  = "+14152222222";

  private  final PushSender             pushSender             = mock(PushSender.class            );
  private  final FederatedClientManager federatedClientManager = mock(FederatedClientManager.class);
  private  final AccountsManager        accountsManager        = mock(AccountsManager.class       );

  private final ReceiptSender receiptSender = new ReceiptSender(accountsManager, pushSender, federatedClientManager);

  private  final ObjectMapper mapper = new ObjectMapper();

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
                                                            .addProvider(AuthHelper.getAuthFilter())
                                                            .addProvider(new AuthValueFactoryProvider.Binder())
                                                            .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                                                            .addResource(new ReceiptController(receiptSender))
                                                            .build();

  @Before
  public void setup() throws Exception {
    Set<Device> singleDeviceList = new HashSet<Device>() {{
      add(new Device(1, null, "foo", "bar", "baz", "isgcm", null, null, false, 111, null, System.currentTimeMillis(), System.currentTimeMillis(), false, false, "Test"));
    }};

    Set<Device> multiDeviceList = new HashSet<Device>() {{
      add(new Device(1, null, "foo", "bar", "baz", "isgcm", null, null, false, 222, null, System.currentTimeMillis(), System.currentTimeMillis(), false, false, "Test"));
      add(new Device(2, null, "foo", "bar", "baz", "isgcm", null, null, false, 333, null, System.currentTimeMillis(), System.currentTimeMillis(), false, false, "Test"));
    }};

    Account singleDeviceAccount = new Account(SINGLE_DEVICE_RECIPIENT, singleDeviceList);
    Account multiDeviceAccount  = new Account(MULTI_DEVICE_RECIPIENT, multiDeviceList);

    when(accountsManager.get(eq(SINGLE_DEVICE_RECIPIENT))).thenReturn(Optional.of(singleDeviceAccount));
    when(accountsManager.get(eq(MULTI_DEVICE_RECIPIENT))).thenReturn(Optional.of(multiDeviceAccount));
  }

  @Test
  public synchronized void testSingleDeviceCurrent() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/receipt/%s/%d", SINGLE_DEVICE_RECIPIENT, 1234))
                 .request()
                 .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(null);

    assertThat(response.getStatus() == 204);

    verify(pushSender, times(1)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(true));
  }

  @Test
  public synchronized void testMultiDeviceCurrent() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target(String.format("/v1/receipt/%s/%d", MULTI_DEVICE_RECIPIENT, 12345))
                 .request()
                 .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
                 .put(null);

    assertThat(response.getStatus() == 204);

    verify(pushSender, times(2)).sendMessage(any(Account.class), any(Device.class), any(Envelope.class), eq(true));
  }


}
